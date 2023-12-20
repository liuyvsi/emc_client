"""TimestampGenerator
功能： 生成时间戳字符串。
方法：
    generate_timestamp(): 返回当前时间的格式化时间戳字符串。

JsonSqliteConverter
功能： 将JSON数据转换为SQLite数据并存储。
方法：
    __init__(filename): 从指定的JSON文件中读取数据，创建SQLite数据库和表格。
    create_table(): 如果表格不存在，则创建表格。
    get_sqlite_type(parameter): 根据参数的格式表达式返回SQLite数据类型。
    insert_data(data): 将数据插入到SQLite表格中。
    retrieve_data(record_id): 检索指定记录ID的数据。
    close_connection(): 关闭SQLite连接。

MessageGenerator
功能： 生成和处理消息，并将其压入不同的处理队列中。
方法：
    run_sender_thread(): 启动一个线程来处理发送消息的队列。
    sum_values(json_list): 计算JSON列表中值的总和。
    mean_values(json_list): 计算JSON列表中值的平均值。
    format_json(json1, json2): 格式化JSON数据。
    run_a5_thread(), run_a30_thread(), run_a60_thread(): 启动不同处理时间间隔的线程。
    load_host_config(): 从配置文件加载主机配置。
    start_generators(): 启动数据生成器。
    generate_data(parameter, message_queue): 生成数据并将其加入消息队列。
    generate_and_push_messages(): 生成消息并压入处理队列。
    merge_json_data(json_data_list): 合并JSON数据。
    stop_generators(): 停止数据生成器。

HostConfig
功能： 存储主机配置信息。
方法：
    __init__(config_data): 从提供的配置数据中初始化主机配置。

Parameter
功能： 存储参数信息。
方法：
    __init__(param_data): 从提供的参数数据中初始化参数信息。
    print_parameter_info(): 打印参数信息。

emcdatagen
功能： 生成编码数据并放入消息队列。
方法：
    __init__(parameter, message_queue): 初始化数据生成器。
    generate_data(): 生成数据并将其加入消息队列。
    stop(): 停止数据生成器。
    generate_encoded_value(parameter_name, min_value, max_value, format_expression): 生成编码值。
这些类共同协作以生成、处理和存储消息。你可以根据需要使用这些类，例如，通过调用 MessageGenerator 中的方法来开始生成和处理消息。"""

import json
import random
import time
import queue
import concurrent.futures
import sqlite3
import datetime

class TimestampGenerator:
   def generate_timestamp(self):
       # 获取当前日期和时间
       now = datetime.datetime.now()
       # 格式化日期和时间为字符串
       timestamp_str = now.strftime("%Y%m%d%H%M%S.%f")
       # 去除小数部分
       timestamp_str = timestamp_str.replace(".", "")
       return timestamp_str



class JsonSqliteConverter:
    def __init__(self,  filename):
        with open(filename, 'r') as file:
            json_data = json.load(file)
        self.dbname = json_data['DBname']
        self.json_data = json_data
        self.conn = sqlite3.connect(self.dbname)
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        # Create table if not exists
        create_table_query = f"CREATE TABLE IF NOT EXISTS {self.json_data['Table']} (id INTEGER PRIMARY KEY, "
        for parameter in self.json_data['LocalParameters']:
            create_table_query += f"{parameter['ParameterName']} {self.get_sqlite_type(parameter)}, "
        create_table_query = create_table_query[:-2] + ")"
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def get_sqlite_type(self, parameter):
        format_expression = parameter['FormatExpression']
        if format_expression.startswith('N') and '.' in format_expression:
            return 'REAL'
        elif format_expression.startswith('N'):
            return 'INTEGER'
        elif format_expression.startswith('C'):
            return 'TEXT'
        elif format_expression.startswith('D'):
            return 'DATE'
        else:
            return 'REAL'

    def insert_data(self, data):
        columns = ', '.join(data.keys())
        values = ', '.join([f'"{value}"' for value in data.values()])
        insert_query = f"INSERT INTO {self.json_data['Table']} ({columns}) VALUES ({values})"
        self.cursor.execute(insert_query)
        self.conn.commit()

    def retrieve_data(self, record_id):
        retrieve_query = f"SELECT * FROM {self.json_data['Table']} WHERE id = {record_id}"
        self.cursor.execute(retrieve_query)
        row = self.cursor.fetchone()
        if row:
            columns = [column[0] for column in self.cursor.description]
            return dict(zip(columns, row))
        return None

    def close_connection(self):
        self.conn.close()



class MessageGenerator:
    def __init__(self):
        self.host_config = self.load_host_config()
        self.queues = []
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.host_config.local_parameters)+5)
        #直接发送机的队列
        self.sender_queue=queue.Queue()
        # 5秒处理程序队列
        self.a5_queue=queue.Queue()
        # 30秒处理程序队列
        self.a30_queue=queue.Queue()
        # 60秒护理程序队列
        self.a60_queue=queue.Queue()

    def run_sender_thread(self):
        def sender_thread():
            while True:
                # 从self.sender_queue里取出一个消息
                message = self.sender_queue.get()

                # 打印消息到屏幕上
                print("Sender Thread - Received message:", message)

                # 休眠一秒钟
                time.sleep(1)

        # 在executor中启动sender_thread
        self.executor.submit(sender_thread)
        
    @staticmethod
    def sum_values(json_list):
        result = {}
        for json_data in json_list:
            for key, value in json_data.items():
                if key in result:
                    result[key] += value
                else:
                    result[key] = value
        return result

    @staticmethod
    def mean_values(json_list):
        result = {}
        for json_data in json_list:
            for key, value in json_data.items():
                if key in result:
                    result[key].append(value)
                else:
                    result[key] = [value]

        for key, values in result.items():
            result[key] = sum(values) / len(values)

        return result
    
    @staticmethod
    def format_json(json1, json2):
        # 解析第一个参数json
        format_dict = {}
        for item in json1['LocalParameters']:
            format_dict[item['ParameterName']] = item['FormatExpression']
        
        # 格式化第二个参数json
        formatted_json = {}
        for key, value in json2.items():
            format_expression = format_dict.get(key)
            if format_expression:
                if format_expression.startswith('C'):
                    formatted_value = str(value)[:int(format_expression[1:])]
                elif format_expression.startswith('N'):
                    if '.' in format_expression:
                        format_parts = format_expression[1:].split('.')
                        integer_part = int(format_parts[0])
                        decimal_part = int(format_parts[1])
                        formatted_value = round(value, decimal_part)
                    else:
                        formatted_value = int(value)
                else:
                    formatted_value = str(value)[:int(format_expression)]
                
                formatted_json[key] = formatted_value
            else:
                formatted_json[key] = value
        
        return formatted_json



    def run_a5_thread(self):
        def a5_thread():
            messages = []  # 用于存储消息
            timer = TimestampGenerator()
            dbconverter = JsonSqliteConverter('/app/emcdatagen/a5conf.json')
       
            while True:
                # 从self.a5_queue里取出一个消息
                message = self.a5_queue.get()
                # 将消息加入列表
                messages.append(message)

                # 如果消息数量达到5条，合并并打印到屏幕上
                if len(messages) == 5:
                    result=self.mean_values(messages)
                #    print("A5 Thread - Average:", result)
                    result["QN"] = timer.generate_timestamp()
                    result = self.format_json(dbconverter.json_data,result)
                    dbconverter.insert_data(result)
                    messages = []  # 重置消息列表

                # 休眠n秒钟
                time.sleep(1)

        # 在executor中启动a5_thread
        self.executor.submit(a5_thread)

    def run_a30_thread(self):
        def a30_thread():
            messages = []  # 用于存储消息
            timer = TimestampGenerator()
            dbconverter = JsonSqliteConverter('/app/emcdatagen/a30conf.json')
       
            while True:
                # 从self.a30_queue里取出一个消息
                message = self.a30_queue.get()
                # 将消息加入列表
                messages.append(message)

                # 如果消息数量达到5条，合并并打印到屏幕上
                if len(messages) == 30:
                    result=self.mean_values(messages)
                #    print("A5 Thread - Average:", result)
                    result["QN"] = timer.generate_timestamp()
                    result = self.format_json(dbconverter.json_data,result)
                    dbconverter.insert_data(result)
                    messages = []  # 重置消息列表

                # 休眠n秒钟
                time.sleep(1)

        # 在executor中启动a30_thread
        self.executor.submit(a30_thread)

    def run_a60_thread(self):
        def a60_thread():
            messages = []  # 用于存储消息
            timer = TimestampGenerator()
            dbconverter = JsonSqliteConverter('/app/emcdatagen/a60conf.json')
       
            while True:
                # 从self.a30_queue里取出一个消息
                message = self.a30_queue.get()
                # 将消息加入列表
                messages.append(message)

                # 如果消息数量达到5条，合并并打印到屏幕上
                if len(messages) == 30:
                    result=self.mean_values(messages)
                #    print("A5 Thread - Average:", result)
                    result["QN"] = timer.generate_timestamp()
                    result = self.format_json(dbconverter.json_data,result)
                    dbconverter.insert_data(result)
                    messages = []  # 重置消息列表

                # 休眠n秒钟
                time.sleep(1)
        # 在executor中启动a60_thread
        self.executor.submit(a60_thread)

    

    def load_host_config(self):
        with open('host_conf.json', 'r') as file:
            config_data = json.load(file)
        return HostConfig(config_data)

    def start_generators(self):
        for parameter in self.host_config.local_parameters:
            message_queue = queue.Queue()
            self.queues.append(message_queue)
            self.executor.submit(self.generate_data, parameter, message_queue)

    def generate_data(self, parameter, message_queue):
        data_generator = emcdatagen(parameter, message_queue)
        data_generator.generate_data()

    def generate_and_push_messages(self):
        try:
            while True:
                messages = []
                for queue in self.queues:
                    message = queue.get()
                    # Assuming message is a JSON object, convert it to a string
                    messages.append(message)
                
              
                json_ch = self.merge_json_data(messages)
                # 数据总线--开始

                self.sender_queue.put(json_ch)
                self.a5_queue.put(json_ch)
                self.a30_queue.put(json_ch)
                self.a60_queue.put(json_ch)
                #数据总线--结束
                
#                print("Combined Message:", combined_message)
        except KeyboardInterrupt:
            self.stop_generators()

    @staticmethod
    def merge_json_data(json_data_list):
        result = {}
        for json_data in json_data_list:
            for key, value in json_data.items():
                if key in result:
                    if isinstance(result[key], list):
                        result[key].append(value)
                    else:
                        result[key] = [result[key], value]
                else:
                    result[key] = value
        return result

    def stop_generators(self):
        self.executor.shutdown()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.host_config.local_parameters))

class HostConfig:
    def __init__(self, config_data):
        self.unique_identifier = config_data["UniqueIdentifier"]
        self.host_name = config_data["HostName"]
        self.local_parameters = [Parameter(param) for param in config_data["LocalParameters"]]

class Parameter:
    def __init__(self, param_data):
        self.parameter_name = param_data["ParameterName"]
        self.format_expression = param_data["FormatExpression"]
        self.normal_max = param_data["NormalMax"]
        self.normal_min = param_data["NormalMin"]
        self.generate_min = param_data["GenerateMin"]
        self.generate_max = param_data["GenerateMax"]
        self.data_generation_interval = param_data["DataGenerationIntervalInSeconds"]

    def print_parameter_info(self):
        print("Parameter Name:", self.parameter_name)
        print("Format Expression:", self.format_expression)
        print("Normal Max:", self.normal_max)
        print("Normal Min:", self.normal_min)
        print("Generate Min:", self.generate_min)
        print("Generate Max:", self.generate_max)
        print("Data Generation Interval (seconds):", self.data_generation_interval)



class emcdatagen:
    def __init__(self, parameter, message_queue):
        self.parameter = parameter
        self.message_queue = message_queue
        self.is_running = True

    def generate_data(self):
        while self.is_running:
            # 生成介于 generate_min 和 generate_max 之间的随机数
            generated_value = self.generate_encoded_value(self.parameter.parameter_name, self.parameter.generate_min, self.parameter.generate_max,self.parameter.format_expression)
            self.message_queue.put(generated_value)
            time.sleep(self.parameter.data_generation_interval)

    def stop(self):
        self.is_running = False
    
    def generate_encoded_value(self, parameter_name, min_value, max_value, format_expression):
        # 生成随机数
        random_value = random.uniform(min_value, max_value)

        # 根据格式表达式进行格式化
        if '.' in format_expression:
            # 提取小数点后的位数
            decimal_places = int(format_expression.split('.')[1])
            formatted_value = round(random_value, decimal_places)
        else:
            formatted_value = int(random_value)

        # 构建返回的 JSON 结构
        result_json = {parameter_name: formatted_value}
        return result_json
    

