import json
import package_mod


# Read protocol JSON from file
file_name = '/app/emcdatagenV1/packdef.json'
protocol_json = package_mod.read_protocol_from_file(file_name)

# Create an instance of NetworkProtocol
network_protocol = package_mod.NetworkProtocol(protocol_json)

# Data to pack
data_to_pack = "Hello, this is the data to be packed!"

# Pack the data
packed_data = network_protocol.pack(data_to_pack)
print("Packed data:", packed_data)

# Unpack the data
try:
    unpacked_data = network_protocol.unpack(packed_data)
    print("Unpacked data:", unpacked_data)
except ValueError as e:
    print("Error during unpacking:", e)