import socket
import hj212_mod  # Assuming hj212_mod.py is in the same directory


class DataProcessor:
    def __init__(self, config_file):
        self.config = self.read_config(config_file)
        self.segmenter = hj212_mod.JSONSegmenter(self.config['ST'], self.config['CN'],
                                                self.config['PW'], self.config['MN'])
        self.network_protocol = hj212_mod.NetworkProtocol(hj212_mod.read_protocol_from_file('packdef.json'))

    @staticmethod
    def read_config(file_name):
        with open(file_name, 'r') as file:
            return hj212_mod.json.load(file)

    def send_data(self, content):
        segmented_data = self.segmenter.segment_data(content)

        for segment in segmented_data:
            str_data = hj212_mod.JSONByteConverter.json_to_bytes(segment)
            packed_data = self.network_protocol.pack(str_data)
            self._send_udp(packed_data, self.config['destin_ip'], self.config['destin_port'])

    def receive_data(self):
        udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_server.bind((self.config['local_ip'], self.config['local_port']))

        received_segments = []
        while True:
            data, _ = udp_server.recvfrom(1024)  # Change the buffer size if needed
            unpacked_data = self.network_protocol.unpack(data.decode('utf-8'))
            received_segments.append(unpacked_data)

            if len(received_segments) == int(received_segments[0]['PNUM']):
                break

        concatenated_data = self.segmenter.concatenate_segments(received_segments)
        return concatenated_data

    @staticmethod
    def _send_udp(data, destin_ip, destin_port):
        udp_client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_client.sendto(data.encode('utf-8'), (destin_ip, destin_port))
        udp_client.close()


# Example usage:
processor = DataProcessor('host_udp_conf.json')

# Sending data
content_to_send = "Your large content here...\
  Your large content here................................................line1\
  Your large content here................................................line2 \
      Your large content here................................................line3 \
          Your large content here................................................line4 \
              Your large content here................................................line5 \
                  Your large content here................................................line6 \
                      Your large content here................................................line7  "
processor.send_data(content_to_send)


