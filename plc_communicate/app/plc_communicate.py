from pymodbus.client import ModbusTcpClient
import logging
from logging.handlers import RotatingFileHandler
import struct
import numpy as np
from redis import StrictRedis
from time import sleep, time
import os
import logging
import random


def read_config():
    config_path = "/config/config.conf"
    with open(config_path, "r") as file:
        return file.read()

def exponential_backoff(service_name, MinDelay=1, MaxDelay=10, Factor=2, Jitter=0.1):
    """
    Декоратор для реализации механизма экспоненциального откладывания при неудачных попытках подключения к сервису.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            delay = MinDelay
            while True:
                try:
                    result = func(*args, **kwargs)
                    if result:
                        return result
                except Exception as e:
                    logging.error(f"Exception during {service_name} connection: {str(e)}")
                
                logging.error(f"Failed to connect to {service_name}. Retrying in {delay:.2f} seconds.")
                sleep(delay)
                delay = min(delay * Factor, MaxDelay)
                delay = delay + random.normalvariate(delay * Jitter)
        return wrapper
    return decorator

class PLC:
    def __init__(self):
        self.config = eval(read_config())
        self.plc_data = {}
        self.error_flag = False
        # Настройка логгирования
        self.setup_logging()
        # Инициализация адресов регистров
        self.initialize_tags()
        # Подключение к PLC и Redis
        self.client = self.connect_plc()
        self.r = self.connect_redis()
        # Параметры
        self.old_time4speed = time()
        self.old_time4acceleration = time()
        self.acceleration = 0
        self.old_speed = 0
        self.old_acceleration_plus = 0
        self.old_acceleration_minus = 0
        self.old_plc_length = None
        self.speed = None
        self.tube_status = False
        self.old_time = time()

    def setup_logging(self):
        """
        Настраивает логгирование с ротацией файлов.
        """
        log_file = "/log/plc_communication.log"
        max_log_size = 10 * 1024 * 1024  # Максимальный размер файла лога в байтах (10 MB)
        backup_count = 3  # Количество резервных файлов логов

        # Настройка логгирования
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        handler = RotatingFileHandler(
            log_file, maxBytes=max_log_size, backupCount=backup_count, encoding='utf-8'
        )
        formatter = logging.Formatter('%(asctime)s|%(name)s|%(levelname)s|%(message)s',
                                      datefmt='%d-%m-%Y %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Отключение логов библиотеки pymodbus
        logging.getLogger("pymodbus").setLevel(logging.CRITICAL)

    def initialize_tags(self):
        """
        Инициализирует теги для чтения и записи из/в PLC.
        """
        self.register_address_read = {
            'plc_speed': [2, 1],
            'plc_length': [4, 1],
            'plc_door_control_cabinet': [6, 1],
            'pause_check': [6, 1],
            'plc_door_control_unit_1': [7, 1],
            'plc_door_control_unit_2': [8, 1],
            'plc_door_control_unit_3': [9, 1],
            'plc_dc_1': [10, 1],
            'plc_dc_2': [11, 1],
            'plc_dc_3': [12, 1],
            'plc_dc_4': [13, 1],
            'plc_door_management_cabinet': [14, 1],
            'plc_defender': [15, 1],
        }
        self.register_address_write = {
            'yellow': 16,
            'red': 17,
            'status': 18,
            'amplitude': 19,
        }

    @exponential_backoff("PLC")
    def connect_plc(self):
        """
        Подключается к PLC и возвращает клиента Modbus.
        """
        client = ModbusTcpClient(
            host=self.config['HOST_PLC'], port=self.config['PORT_PLC']
        )
        return client if client.connect() else None

    @exponential_backoff("Redis")
    def connect_redis(self):
        """
        Подключается к Redis и возвращает клиента Redis.
        """
        r = StrictRedis(
            host=self.config['HOST_REDIS'], port=self.config['PORT_REDIS'], password=self.config['PASSWORD']
        )
        return r if r.ping() else None

    def read_register(self):
        """
        Читает значения регистров из PLC и сохраняет их в `self.plc_data`.
        Обрабатывает ошибки при чтении регистров и записывает их в лог.
        """
        for key, x in self.register_address_read.items():
            try:
                self.response = self.client.read_holding_registers(
                    x[0], count=x[1])
                self.plc_data[key] = [self.response, x[1]]
                self.error_flag = False
            except Exception as e:
                if not self.error_flag:
                    error_msg = f"Error reading registers: {e}"
                    logging.error(msg=error_msg)
                    self.client = self.connect_plc()

    def encode_data(self):
        """
        Кодирует данные из регистров в нужный формат.
        Обрабатывает данные в зависимости от их типа и записывает их в `self.plc_data`.
        """
        if not self.error_flag:
            for key, x in self.plc_data.items():
                if x[1] > 1:
                    byte_string = struct.pack('<HH', *x[0].registers)
                    dt = np.dtype(np.float32)
                    dt = dt.newbyteorder('<')
                    self.plc_data[key] = str(
                        round((np.frombuffer(bytearray(byte_string), dtype=dt)[0]), 2))
                else:
                    if key == 'plc_length':
                        self.plc_data[key] = str(int(x[0].registers[0]) / 100)
                    elif key == 'plc_speed':
                        self.plc_data[key] = str(int(x[0].registers[0]) / 10)
                    else:
                        self.plc_data[key] = str(x[0].registers[0])
            #logging.info(msg=self.plc_data)
            #print(self.plc_data)

    def calculate_speed(self):
        """
        Вычисляет скорость на основе данных о длине. Обновляет значения скорости и времени.
        Если данные не изменились, скорость устанавливается в 0.
        """
        try:
            if float(self.plc_data["plc_length"]) != self.old_plc_length:
                self.speed += ((float(self.plc_data["plc_length"]) - self.old_plc_length) /
                               float(time() - self.old_time4speed)) / 2
                self.old_time4speed = time()
                self.old_plc_length = float(self.plc_data["plc_length"])
        except Exception as e:
            print(e)
            self.old_plc_length = float(self.plc_data["plc_length"])
            self.speed = 0
        self.plc_data['plc_speed'] = self.speed

    def calculate_acceleration(self):
        """
        Вычисляет ускорение на основе изменений скорости. Записывает значения ускорения в лог,
        если ускорение увеличилось или уменьшилось.
        """
        if self.speed != self.old_speed:
            self.acceleration = (self.speed - self.old_speed) / \
                float(time() - self.old_time4acceleration)
            self.old_time4acceleration = time()
            self.old_speed = self.speed
            print('acceleration = ', self.acceleration)
        if abs(self.acceleration) > abs(self.old_acceleration_plus) and self.acceleration > 0:
            logging.info(msg=f'ускорение = {self.acceleration} м/с')
            self.old_acceleration_plus = self.acceleration
        if abs(self.acceleration) > abs(self.old_acceleration_minus) and self.acceleration < 0:
            logging.info(msg=f'ускорение = {self.acceleration} м/с')
            self.old_acceleration_minus = self.acceleration

    def write2redis(self):
        """
        Записывает данные из `self.plc_data` в Redis. Обрабатывает ошибки при записи и записывает их в лог.
        """
        try:
            self.r.hmset('plc', self.plc_data)
            self.error_flag = True
        except Exception as e:
            if not self.error_flag:
                self.r = self.connect_redis()  

    def check_defect(self):
        """
        Проверяет наличие дефектов на основе данных из Redis и активного флага управления.
        Возвращает `True`, если дефект обнаружен, иначе `False`.
        """
        try:
            data = eval(self.r.get('profile_and_measurements'))
            for defect_present in data['defects'].values():
                if defect_present != 0 and int(self.r.get('control_active_flag')) == 1:
                    return True
        except:
            return False

    def check_camera_connection(self):
        """
        Проверяет подключение к камерам.
        """
        for i in range(1, 5):
            if os.system('ping -c 1 192.168.XXX.XXX{}'.format(i)) != 0:
                return True
        else:
            return False

    def check_amplitude(self):
        """
        Проверяет амплитуду на основе данных из Redis.
        Возвращает `True`, если амплитуда превышает 2000 и активен флаг управления, иначе `False`.
        """
        try:
            data = eval(self.r.get('profile_and_measurements'))
            if data['defects']['амплитуда'] > 2000 and int(self.r.get('control_active_flag')) == 1:
                return True
            else:
                return False
        except:
            return False

    def defect_status(self):
        """
        Проверяет статус дефекта на основе значения из Redis. Если дефект обнаружен
        и прошло больше 5 секунд с последнего обновления, обновляет флаг и возвращает `True`.
        Иначе возвращает `False`.
        """
        value = self.r.get('filtered_defect_flag')
        if value is not None and int(value) == 1 and int(time() - self.old_time) > 5:
            self.r.set('filtered_defect_flag', 0)
            self.old_time = time()
            return True
        else:
            return False

    def write2plc(self):
        """
        Записывает данные в PLC в зависимости от различных условий, таких как наличие дефекта,
        амплитуда и статус управления. Управляет состоянием лампы и другими параметрами.
        """
        # условие желтой лампы
        print(self.defect_status())
        if self.defect_status():
            self.client.write_register(address=self.register_address_write['yellow'], value=1)
        else:
            self.client.write_register(address=self.register_address_write['yellow'], value=0)

        # Условие красной лампы
        if False:
            self.client.write_register(address=self.register_address_write['red'], value=1)
        else:
            self.client.write_register(address=self.register_address_write['red'], value=0)

        # статус контроля
        value = self.r.get('control_active_flag')
        if value is not None and int(value) == 1:
            self.client.write_register(address=self.register_address_write['status'], value=1)
        else:
            self.client.write_register(address=self.register_address_write['status'], value=0)

        # амплитуда
        if self.check_amplitude():
            self.client.write_register(address=self.register_address_write['amplitude'], value=1)
        else:
            self.client.write_register(address=self.register_address_write['amplitude'], value=0)

    def get_plc_data(self):
        """
        Основной цикл работы системы. Читает данные из регистров, кодирует их, записывает в Redis,
        и управляет PLC. Выполняется бесконечно с интервалом в 1 секунду.
        """
        logging.info(msg='Starting the system')
        print('start')
        while True:
            self.read_register()
            self.encode_data()
            self.write2redis()
            self.write2plc()
            sleep(1)


if __name__ == '__main__':
    a = PLC()
    a.get_plc_data()
