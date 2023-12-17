import socket
import time
import json
import hj212_mod
#from hj212_mod import JSONSegmenter, NetworkProtocol

# Function to read a large file
def read_large_file(file_path):
    with open(file_path, 'rb') as file:
        return file.read().decode('utf-8')  # Assuming file is text-based

# Initialize UDP socket
UDP_IP = '172.17.0.3'  # Replace with your receiver's IP
UDP_PORT = 12345  # Replace with your receiver's port
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Read large file
file_content = read_large_file('host_jason_test8.py')
#file_content = read_large_file('test.json')

# Initialize JSONSegmenter
segmenter = hj212_mod.JSONSegmenter(ST='YourST', CN='YourCN', PW='YourPW', MN='YourMN')
segments = segmenter.segment_data(file_content)

# Initialize NetworkProtocol
protocol_data = hj212_mod.read_protocol_from_file('packdef.json')
network_protocol = hj212_mod.NetworkProtocol(protocol_data)

# Send segments using UDP
for segment in segments:
#   packed_data = network_protocol.pack(json.dumps(segment))
    packed_data = network_protocol.pack(hj212_mod.JSONByteConverter.json_to_bytes(segment))
    sock.sendto(packed_data.encode('utf-8'), (UDP_IP, UDP_PORT))
    time.sleep(0.1)  # Adding a small delay between sends for stability

sock.close()



