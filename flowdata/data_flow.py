"""
通用跑数任务流
"""

from collections import Counter
from typing import Generator

from ._logger import logger
from .data_parallel import DataParallel
from .decorator import err_catch, interrupt_catch, timer, tps
from .task import TASK_LIST, Task


class FlowBase:
    def __init__(self, verbose=True):
        self.verbose = verbose
        self.counter = Counter()

    def get_data(self) -> Generator[dict, None, None]:
        """获取数据"""
        raise NotImplementedError("get_data: not implemented!")

    @tps(step=20)
    def save_data(self, item_iter):
        """保存数据"""
        raise NotImplementedError("save_data: not implemented!")

    def clip_data(self, item_iter, offset: int = 0, head_num: int = None):
        """偏移及截断处理"""
        num = 0
        for i, item in enumerate(item_iter):

            if i < offset:
                continue
            num += 1
            if head_num and num > head_num:
                break

            if self.verbose:
                logger.info("准备处理第%s条数据", i)

            yield item

    def count_data(self, item_iter):
        """任务执行统计"""
        for item in item_iter:
            self.counter["total_num"] += 1
            if not item:
                self.counter["error_num"] += 1
                continue

            yield item

    def _exec(self, item_iter, task: Task):
        """单进程"""
        for i, item in enumerate(item_iter):
            task_func = getattr(self, task.func_name)
            item = task_func(item)
            yield item

    def _exec_mp(self, item_iter, task: Task):
        """多进程"""
        task_func = getattr(self, task.func_name)
        item_iter_fn = lambda: item_iter
        with DataParallel(
            item_iter_fn=item_iter_fn, work_num=task.work_num, process_fn=task_func
        ) as t:
            for index, item in enumerate(t.send_data()):
                yield item

    def exec_task(self, item_iter, task: Task):
        """执行单task"""
        if task.work_num > 1:
            item_iter = self._exec_mp(item_iter, task)
        else:
            item_iter = self._exec(item_iter, task)

        return item_iter

    def exec_tasks(self, item_iter):
        """执行多task， TASK_LIST中上一个task的输出是下一个task的输入"""
        for task in TASK_LIST:
            if task.class_name != self.__class__.__name__:
                continue
            if not hasattr(self, task.func_name):
                continue
            item_iter = self.exec_task(item_iter, task)

        return item_iter

    def print_task(self):
        num = 0
        for task in TASK_LIST:
            if task.class_name != self.__class__.__name__:
                continue
            num += 1
            logger.info("task_%s: %s", num, task)

    @timer("main")
    @interrupt_catch
    def main(self, offset: int = 0, head_num: int = None):
        """主函数

        Args:
            offset (int, optional): 偏移量. Defaults to 0.
            head_num (int, optional): 要跑的数据总量. Defaults to None.
        """
        self.print_task()
        item_iter = self.get_data()
        item_iter = self.clip_data(item_iter, offset, head_num)
        item_iter = self.exec_tasks(item_iter)
        item_iter = self.count_data(item_iter)
        self.save_data(item_iter)

        print()
        logger.info("数据执行统计: %s", self.counter)
        logger.info("finish")
