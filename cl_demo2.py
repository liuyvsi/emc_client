import socket
import hj212_mod  # Assuming hj212_mod.py is in the same directory

# Example usage:
processor = hj212_mod.DataProcessor('host_udp_conf.json')

# Sending data
content_to_send = "Your large content here...\
  Your large content here................................................line1\
  Your large content here................................................line2 \
      Your large content here................................................line3 \
          Your large content here................................................line4 \
              Your large content here................................................line5 \
                  Your large content here................................................line6 \
    Your large content here................................................line2 \
      Your large content here................................................line3 \
          Your large content here................................................line4 \
              Your large content here................................................line5 \
                  Your large content here................................................line6 \
    Your large content here................................................line2 \
      Your large content here................................................line3 \
          Your large content here................................................line4 \
              Your large content here................................................line5 \
                  Your large content here................................................line6 \
    Your large content here................................................line2 \
      Your large content here................................................line3 \
          Your large content here................................................line4 \
              Your large content here................................................line5 \
                  Your large content here................................................line6 \
                      Your large content here................................................line7  "
processor.send_data(content_to_send)


