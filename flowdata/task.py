
import functools
from dataclasses import dataclass, field
from typing import List


@dataclass
class Task:
    """任务对象"""
    func_name:str = field(default=None, metadata={"help": "任务函数，由add_task添加至task_list"})
    work_num:int = field(default=1, metadata={"help": "进程数量"})


TASK_LIST: List[Task] = []

# 添加至任务列表
def add_task(work_num:int=1):
    def _add_task(func):
        TASK_LIST.append(
            Task(func.__name__, work_num)
        )
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return _add_task

