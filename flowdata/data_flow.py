"""
通用跑数任务流
"""

import heapq
from collections import Counter
from typing import Generator

from ._logger import logger
from .data_parallel import DataParallel
from .decorator import err_catch, interrupt_catch, timer, tps
from .task import TASK_LIST, Task, get_max_work_nums


class FlowBase:
    def __init__(self, verbose=True, keep_order=False):
        """[summary]

        Args:
            verbose ([bool]): [是否打印日志]
            keep_order ([bool]): [是否按照输入顺序返回，True时，无法流式保存数据]
        """
        self.verbose = verbose
        self.keep_order = keep_order
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

            item["__origin_id"] = num
            num += 1
            if head_num and num > head_num:
                break

            if self.verbose:
                logger.info("准备处理第%s条数据", i)

            yield item

    def count_data(self, item_iter):
        """任务执行统计
        过滤掉 None 的 item
        """
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
            item_iter_fn=item_iter_fn,
            work_num=task.work_num,
            process_fn=task_func,
            dummy=task.dummy,
        ) as _item_iter:
            for index, item in enumerate(_item_iter):
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

    def _keep_order(self, item_iter):
        max_work_nums = get_max_work_nums()  # 任务中最大进程数量
        heap_items = []
        order_id = 0  # 当前应该返回的item id

        def get_order_item(item):
            nonlocal order_id

            # 当前 item 顺序正确，直接返回
            if item["__origin_id"] == order_id:
                order_id += 1
                return item

            # 当前 item 顺序不正确，放入堆中
            heapq.heappush(heap_items, (item["__origin_id"], item))
            # 从堆中取出一个 h_item
            _, h_item = heapq.heappop(heap_items)
            if h_item["__origin_id"] == order_id:
                order_id += 1
                return h_item

            # h_item 顺序也不正确，再次放入堆中
            heapq.heappush(heap_items, (h_item["__origin_id"], h_item))

            # heap_items 累积过多 items 时，强制更改 order_id
            if len(heap_items) >= max_work_nums + 3:
                _, h_item = heapq.heappop(heap_items)
                order_id = h_item["__origin_id"]
                return h_item

        for item in item_iter:
            _item = get_order_item(item)
            if _item:
                yield _item

        while heap_items:
            _, h_item = heapq.heappop(heap_items)
            yield h_item

    def rm_keys(self, item_iter):
        """去掉多余key"""
        for item in item_iter:
            item.pop("__origin_id")
            yield item

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

        if self.keep_order:
            item_iter = self._keep_order(item_iter)

        item_iter = self.rm_keys(item_iter)
        self.save_data(item_iter)

        print()
        logger.info("数据执行统计: %s", self.counter)
        logger.info("finish")
