class emc_cp_convert:
    def __init__(self, file_name="/app/emcdatagen/converter.json"):
        self.config = self.load_config(file_name)

    def load_config(self, file_name):
        import json
        with open(file_name, 'r') as file:
            config = json.load(file)
        return config

    def check_header_tail(self, input_string):
        return input_string.startswith(self.config["head"]) and input_string.endswith(self.config["tail"])

    def convert_to_json(self, input_string):
        if not self.check_header_tail(input_string):
            return "Invalid header or tail"

        input_string = input_string[len(self.config["head"]):-len(self.config["tail"])]
        parameters = {}
        splits = input_string.split(self.config["Spliter"])
        for split in splits:
            param, value = split.split(self.config["Connector"])
            param = param.strip()
            value = value.strip()

            for local_param in self.config["LocalParameters"]:
                if local_param["ParameterName"] == param:
                    parameters[param] = self.format_value(value, local_param["FormatExpression"])
                    break

        return parameters

    def format_value(self, value, format_expression):
        if format_expression.startswith("N"):
            decimal_places = 0
            if "." in format_expression:
                decimal_places = int(format_expression.split(".")[1])

            if decimal_places > 0:
                try:
                    value = round(float(value), decimal_places)
                except ValueError:
                    value = 0.0
            else:
                try:
                    value = int(float(value))
                except ValueError:
                    value = 0
        elif format_expression.startswith("C"):
            value = str(value)[:int(format_expression[1:])]
        elif format_expression.startswith("F"):
            decimal_places = int(format_expression.split(".")[1])
            try:
                value = round(float(value), decimal_places)
            except ValueError:
                value = 0.0

        return value

    def convert_to_string(self, input_json):
        output_string = self.config["head"]
        for param, value in input_json.items():
            for local_param in self.config["LocalParameters"]:
                if local_param["ParameterName"] == param:
                    output_string += f"{param}{self.config['Connector']}{self.format_output_value(value, local_param['FormatExpression'])}{self.config['Spliter']}"

        output_string = output_string.rstrip(self.config["Spliter"]) + self.config["tail"]
        return output_string

    def format_output_value(self, value, format_expression):
        if format_expression.startswith("N"):
            decimal_places = 0
            if "." in format_expression:
                decimal_places = int(format_expression.split(".")[1])

            if decimal_places > 0:
                value = f"{float(value):.{decimal_places}f}"
            else:
                value = str(int(value))
        elif format_expression.startswith("C"):
            value = str(value)[:int(format_expression[1:])]
        elif format_expression.startswith("F"):
            decimal_places = int(format_expression.split(".")[1])
            value = f"{float(value):.{decimal_places}f}"

        return value



# 初始化对象
converter = emc_cp_convert()

# 从字符串转换为JSON
input_string = "CP=&&Parameter1=93.245678;Parameter2=hello are u ok;Parameter3=399.77;Parameter4=6.30333&&"
output_json = converter.convert_to_json(input_string)
print(output_json)

# 从JSON转换为字符串
#input_json = {"Parameter1": 93.2, "Parameter2": "hello", "Parameter3": 99, "Parameter4": 6.3}
input_json = output_json
output_string = converter.convert_to_string(input_json)
print(output_string)
