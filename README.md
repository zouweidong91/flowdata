# flowdata
流式数据简易处理工具


本项目支持对流式数据的处理进行多进程/线程加速。


# 安装 flowdata

```
pip install flowdata
```


# Quick Start
## 数据读取支持
* 简单封装了txt, json, jsonl, excel文件的读写接口。请参考FileTool, JsonTool, JsonlTool, ExcelTool类。

## 1、单任务
* 代码中通过add_task将任务加载到任务流中，且可以根据需要指定不同进程/线程数量

* add_task: dummy 默认为 False，参数设置为 True 开启线程模式。

* 流式处理返回结果默认是无序的。如果需要有序返回需指定参数: keep_order=True

```python
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

TaskFlow(verbose=False, keep_order=True).main()
```

## 2、多任务
* 假设一个处理数据的任务可以细分为多个子任务，例如，task_a, task_b。任务执行按照task的添加顺序执行。
* 前一个任务的输出是下一个任务的输入。

```python
import time
import unittest
import random

from flowdata import FlowBase, add_task
from flowdata.decorator import err_catch


# 多个任务
class TaskFlow(FlowBase):

    @err_catch()
    @add_task(work_num=2)
    def task_1(self, item: dict, *args, **kwargs) -> dict:
        time.sleep(random.random())
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
        for i in range(100):
            yield {"id": i}

    def save_data(self, item_iter):
        for item in item_iter:
            print(item)

TaskFlow().main()
```

## 3、multigpu任务
* gpu任务，无法在子进程中使用主进程创建的模型，因此需要切换至线程模式

```python
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

TaskFlow(verbose=True).main()
```
