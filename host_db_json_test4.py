"""从5进行改写，将打印改写成为压入相应处理队列中。"""

import emc_frame_mod

class client_main(emc_frame_mod.MessageGenerator):
    def run_sender_thread(self):
        return super().run_sender_thread() #printing, change this for sending function


# 主程序
if __name__ == "__main__":
    message_generator = emc_frame_mod.MessageGenerator()
    message_generator.start_generators()
    message_generator.run_sender_thread()
    message_generator.run_a5_thread()
    message_generator.run_a30_thread()
    message_generator.run_a60_thread()
    message_generator.generate_and_push_messages()