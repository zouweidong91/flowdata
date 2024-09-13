import time
import unittest
import random
from flowdata import FlowBase, add_task
from flowdata.decorator import err_catch


# 单个任务
class TaskFlow(FlowBase):

    @add_task(work_num=4, dummy=False)
    @err_catch()
    def add_1(self, item: dict, *args, **kwargs) -> dict:
        time.sleep(random.random())
        item["r"] = 2
        if item["id"] == 5:
            raise Exception("ha")
        return item

    def get_data(self):
        for i in range(30):
            yield {"id": i}

    def save_data(self, item_iter):
        for item in item_iter:
            print(item)


class FlowTest(unittest.TestCase):
    def test_flow(self):
        TaskFlow(verbose=False, keep_order=True).main()


if __name__ == "__main__":

    unittest.main()
