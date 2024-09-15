import socket
import matplotlib.pyplot as plt
import numpy as np


class Lidar:
    def __init__(self, ip, port):
        self.lider_ip = ip
        self.lider_port = port
        self.lider_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.start_measurement_command = bytes.fromhex("02 73 45 4E 20 4C 4D 44 73 63 61 6E 64 61 74 61 20 31 03")                      # sEN LMDscandata 1
        self.active_mean_filter = bytes.fromhex("02 73 57 4E 20 4C 46 50 6D 65 61 6E 66 69 6C 74 65 72 20 31 20 2B 31 30 20 30 03")     # sWN LFPmeanfilter 1 +10 0
        self.inactive_mean_filter = bytes.fromhex("02 73 57 4E 20 4C 46 50 6D 65 61 6E 66 69 6C 74 65 72 20 30 20 2B 31 30 20 30 03")   # sWN LFPmeanfilter 0 +10 0
        self.login = bytes.fromhex("02 73 4D 4E 20 53 65 74 41 63 63 65 73 73 4D 6F 64 65 20 30 34 20 38 31 42 45 32 33 41 41 03")      # sMN SetAccessMode 04 81BE23AA
        self.run_mes = bytes.fromhex("02 73 4D 4E 20 52 75 6E 03")                                                                      # sMN Run
        self.result_3d_array = []  # Список для хранения двумерных массивов

    def parse_data_packet(self, packet):
        """Обработка полученного пакета данных"""
        try:
            packet = (packet.decode('utf-8')).split()
        except UnicodeDecodeError:
            return None  # Пропускаем пакеты, которые не могут быть декодированы
        
        return packet

    def start_lidar_measurement(self, lidar_mean_filter):
        """Отправка команд"""
        self.lider_socket.connect((self.lider_ip, self.lider_port))

        if lidar_mean_filter:
            #Авторизация в системе для изменения настроек
            self.lider_socket.sendall(self.login)
            packet = self.lider_socket.recv(25)
            packet = self.parse_data_packet(packet)
            #Проверка ответа
            if packet[0].find('sAN') == -1:       
                return None
            #Включение фильтра
            self.lider_socket.sendall(self.active_mean_filter) 
            packet = self.lider_socket.recv(25)
            packet = self.parse_data_packet(packet)
            #Проверка ответа
            if packet[0].find('sWA') == -1:       
                return None
            #Команда для продолжения работы
            self.lider_socket.sendall(self.run_mes) 
        else:
            #Авторизация в системе для изменения настроек
            self.lider_socket.sendall(self.login)
            packet = self.lider_socket.recv(25)
            packet = self.parse_data_packet(packet)
            #Проверка ответа
            if packet[0].find('sAN') == -1:
                return None
            #Выключение фильтра
            self.lider_socket.sendall(self.inactive_mean_filter)
            packet = self.lider_socket.recv(25)
            packet = self.parse_data_packet(packet)
            #Проверка ответа
            if packet[0].find('sWA') == -1:
                return None
            #Команда для продолжения работы
            self.lider_socket.sendall(self.run_mes)
            
        self.lider_socket.sendall(self.start_measurement_command)

    def extract_lidar_info(self, packet, packets):
        """Извлечение информации из пакета данных"""
        if packet[0].find('sSN') == -1:
            return None
        
        index = packet.index('DIST1')
        if index == -1:
            return None

        Scale_factor = packet[index + 1]
        Scale_offset = packet[index + 2]
        Start_angle = int(packet[index + 3], 16) / 10000
        Angular_resolution = int(packet[index + 4], 16) / 10000
        Amount_of_data = int(packet[index + 5], 16)
        
        data_list = packet[index + 6 : index + 6 + Amount_of_data]
        result_measur = [[packets, i, (int(byte, 16)) / 10] for i, byte in enumerate(data_list) if int(byte, 16) != 0]
        
        return Scale_factor, Scale_offset, Start_angle, Angular_resolution, Amount_of_data, result_measur

    def process_measurements(self, result_measur, Start_angle, Angular_resolution):
        """Обработка измерений для отображения"""
        angles = [np.deg2rad(Start_angle + i * Angular_resolution) for i in range(len(result_measur))]
        distances = [point[2] for point in result_measur]

        x = [r * np.cos(theta) for r, theta in zip(distances, angles)]
        y = [r * np.sin(theta) for r, theta in zip(distances, angles)]

        return x, y
    
    def display_data(self, x, y):
        """Отображение данных"""
        plt.figure()
        plt.scatter(x, y, color='b', s = 0.5)
        plt.xlabel('X (мм)')
        plt.ylabel('Y (мм)')
        plt.grid(True)
        plt.axis('equal')
        plt.gca().invert_yaxis()
        plt.show()

    def print_lidar_info(self, Scale_factor, Scale_offset, Start_angle, Angular_resolution, Amount_of_data):
        """Вывод информации о настройках"""
        print("Scale_factor:", Scale_factor, "\n",
              "Scale_offset:", Scale_offset, "\n",
              "Start_angle:", Start_angle, "\n",
              "Angular_resolution:", Angular_resolution, "\n",
              "Amount_of_data:", Amount_of_data, "\n")

    def receive_and_process_data(self, num_of_packets, verbose):
        """Получение и обработка данных"""
        packets = 0
        while num_of_packets > packets:
            packet = self.lider_socket.recv(1876)
            packet = self.parse_data_packet(packet)
            if packet:
                lidar_info = self.extract_lidar_info(packet, packets)
                if lidar_info:
                    Scale_factor, Scale_offset, Start_angle, Angular_resolution, Amount_of_data, result_measur = lidar_info
                    if verbose:
                        self.print_lidar_info(Scale_factor, Scale_offset, Start_angle, Angular_resolution, Amount_of_data)
                    if result_measur:
                        if verbose:
                            display_info = self.process_measurements(result_measur, Start_angle, Angular_resolution)
                            x, y = display_info
                            self.display_data(x, y)
                        self.result_3d_array.append(result_measur)
                        packets += 1

        # Преобразование списка в трехмерный массив
        self.result_3d_array = np.array(self.result_3d_array)

    def close(self):
        """Закрытие соединения"""
        self.lider_socket.close()


# Основной код
lidar_ip = "xxx.xxx.xx.xxx"
lidar_port = 2112

#Вкл./выкл. фильтр
lidar_mean_filter = 0

lidar = Lidar(lidar_ip, lidar_port)

# Команда запуска измерений
lidar.start_lidar_measurement(lidar_mean_filter)

try:
    # Получаем и обрабатываем данные
    lidar.receive_and_process_data(2, 1)
except KeyboardInterrupt:
    print("Программа прервана.")
finally:
    lidar.close()

print(lidar.result_3d_array)
