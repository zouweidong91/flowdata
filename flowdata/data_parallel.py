import multiprocessing as mp
from enum import Enum
from typing import List

from .decorator import interrupt_catch


class FLAG(Enum):
    END = "[end]"  # 进程结束标志


MAX_QUEUE_SIZE = 3


class DataParallel:
    """数据流多进程并行处理逻辑"""

    def __init__(
        self, item_iter_fn, work_num: int, process_fn: callable, *args, **kwargs
    ):
        """[summary]

        Args:
            item_iter_fn ([callable]): [输入数据迭代器fn]
            work_num ([int]): [执行任务进程数量]
            process_fn (callable): [执行函数,外部传入]
        """
        self.item_iter_fn = item_iter_fn
        self.work_num = work_num
        self.process_fn = process_fn
        self.args = args
        self.kwargs = kwargs

        self.queue_in = mp.Queue(MAX_QUEUE_SIZE)  # 接收数据的queue
        self.queue_out = mp.Queue(MAX_QUEUE_SIZE)  # 数据处理后输出queue
        self.p_list: List[mp.Process] = []

    @interrupt_catch
    def recv_data(self):
        """数据放入队列"""
        for item in self.item_iter_fn():
            self.queue_in.put(item)
        self.queue_in.put(FLAG.END)  # 队列放入终止标志

    def send_data(self):
        """数据从队列取出"""
        while True:
            data = self.queue_out.get(block=True, timeout=None)
            if data == FLAG.END:
                break
            yield data

    @interrupt_catch
    def work(self, work_done_value, work_i: int):
        """[summary]

        Args:
            work_done_value ([type]): [跟踪子进程是否结束]
            work_i ([int]): [进程索引id，外部任务可能用到]
        """
        while True:
            data = self.queue_in.get(block=True, timeout=None)
            if data == FLAG.END:
                self.queue_in.put(FLAG.END)  # 解决多进程退出问题
                break

            data = self.process_fn(data, work_i=work_i, *self.args, **self.kwargs)
            self.queue_out.put(data)

        with work_done_value.get_lock():
            work_done_value.value += 1

            if work_done_value.value >= self.work_num:
                self.queue_out.put(FLAG.END)  # 队列放入终止标志

    def run(self):
        # 接收数据进程
        recv_p = mp.Process(target=self.recv_data)
        recv_p.start()
        self.p_list.append(recv_p)

        # 处理数据进程
        work_done_value = mp.Value("i", 0)
        for work_i in range(self.work_num):
            p = mp.Process(target=self.work, args=(work_done_value, work_i))
            p.start()
            self.p_list.append(p)

    def __enter__(self):
        self.run()
        return self

    def __exit__(self, *args, **kwargs):
        # 结束所有进程
        for p in self.p_list:
            if p.is_alive():
                p.terminate()  # 终止活跃进程
            p.join()  # 等待进程结束
