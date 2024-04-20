# flowdata
流式数据简易处理工具

本项目支持对流式数据的处理进行多进程加速


# 安装 flowdata

```
pip install flowdata
```


# Quick Start

## 1、单任务
代码中通过add_task将任务加载到任务流中，且可以根据需要指定不同进程数量

```python

from flowdata import FlowBase
from flowdata import add_task

class TaskFlow(FlowBase):
    @add_task(work_num=1)
    def task(self, item: dict, *args, **kwargs) -> dict:
        time.sleep(.2)
        item['id'] += 1
        return item

    def get_data(self):
        for i in range(20):
            yield {"id": i}

    def save_data(self, item_iter):
        list(item_iter)

TaskFlow().main()
```

## 2、多任务
假设一个处理数据的任务可以细分为多个子任务，例如，task_a, task_b。

```python

from flowdata import FlowBase
from flowdata import add_task

# 多个任务
class TaskFlow(FlowBase):
    @add_task(work_num=2)
    def task_a(self, item: dict, *args, **kwargs) -> dict:
        time.sleep(.2)
        item['id'] += 1
        return item

    @add_task(work_num=2)
    def task_b(self, item: dict, *args, **kwargs) -> dict:
        time.sleep(.2)
        item['id'] += 1
        return item

    def get_data(self):
        for i in range(20):
            yield {"id": i}

    def save_data(self, item_iter):
        list(item_iter)

TaskFlow().main()
```