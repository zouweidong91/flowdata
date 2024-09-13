import time
import unittest

import torch
import torch.nn.functional as F
import torch.nn as nn
from tqdm import tqdm

from flowdata import FlowBase, add_task
from flowdata.decorator import err_catch


class SimpleModel(nn.Module):
    def __init__(self, input_size, hidden_size, num_classes):
        super(SimpleModel, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.fc2 = nn.Linear(hidden_size, hidden_size)
        self.fc3 = nn.Linear(hidden_size, num_classes)

    def forward(self, x):
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x


# 单个任务
class TaskFlow(FlowBase):
    def __init__(self, verbose=True):
        super().__init__(verbose)
        self.init_models()

    def init_models(self):
        device_ids = [0, 1, 2, 3]
        self.num_gpus = len(device_ids)
        self.models = [
            (SimpleModel(100, 2000, 2).cuda(device_id), device_id)
            for device_id in device_ids
        ]

    @add_task(work_num=16, dummy=True)
    @err_catch()
    def add_1(self, item: dict, work_i: int, *args, **kwargs) -> dict:
        time.sleep(0.2)
        index = work_i % self.num_gpus
        model, device_id = self.models[index]
        ipt = item["ipt"]
        rst = model(ipt.to(device_id))
        # print(rst)
        item["rst"] = rst
        return item

    def get_data(self):
        for i in tqdm(range(2000)):
            yield {"id": i, "ipt": torch.randn(2, 100).cuda()}

    def save_data(self, item_iter):
        list(item_iter)


class FlowTest(unittest.TestCase):
    def test_flow(self):
        TaskFlow(verbose=False).main()


if __name__ == "__main__":

    unittest.main()
