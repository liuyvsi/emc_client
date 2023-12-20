"""从5进行改写，将打印改写成为压入相应处理队列中。"""

import emc_frame_mod
import hj212_mod
import time


class client_main(emc_frame_mod.MessageGenerator):
    def run_sender_thread(self):
        processor = hj212_mod.DataProcessor('host_udp_conf.json')

        def sender_thread():
            while True:
                # 从self.sender_queue里取出一个消息
                message = self.sender_queue.get()

                processor.send_data(message)
                print(message)

                # 休眠一秒钟
                time.sleep(1)
        # 在executor中启动sender_thread
        self.executor.submit(sender_thread)

# 主程序
if __name__ == "__main__":
    message_generator = client_main()
    message_generator.start_generators()
    message_generator.run_sender_thread()
    message_generator.run_a5_thread()
    message_generator.run_a30_thread()
    message_generator.run_a60_thread()
    message_generator.generate_and_push_messages()