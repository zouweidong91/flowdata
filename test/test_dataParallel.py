import time
import unittest

from flowdata import DataParallel


class DataParallelTest(unittest.TestCase):
    def item_iter_fn(self):
        for i in range(20):
            yield {"id": i}

    def process_fn(self, item, *args, **kwargs):
        time.sleep(0.2)
        item["id"] += 1
        return item

    def test_dataParallel(self):
        with DataParallel(
            item_iter_fn=self.item_iter_fn,
            work_num=2,
            process_fn=self.process_fn,
            dummy=False,
        ) as data_iter:
            for index, item in enumerate(data_iter):
                print(item)


if __name__ == "__main__":
    unittest.main()
