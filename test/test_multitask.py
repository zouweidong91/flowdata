import time
import unittest

from flowdata import FlowBase, add_task
from flowdata.decorator import err_catch


# 多个任务
class TaskFlow(FlowBase):

    @err_catch()
    @add_task(work_num=2)
    def task_1(self, item: dict, *args, **kwargs) -> dict:
        time.sleep(0.2)
        item["id"] += 1
        if item["id"] == 5:
            raise Exception("ha")
        return item

    @err_catch()
    @add_task(work_num=2)
    def task_2(self, item: dict, *args, **kwargs) -> dict:
        time.sleep(0.2)
        item["id"] += 1
        return item

    def get_data(self):
        for i in range(20):
            yield {"id": i}

    def save_data(self, item_iter):
        list(item_iter)


class FlowTest(unittest.TestCase):
    def test_flow(self):
        TaskFlow(verbose=True).main()


if __name__ == "__main__":
    unittest.main()
