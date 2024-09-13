import functools
from dataclasses import dataclass, field
from typing import List


@dataclass
class Task:
    """任务对象"""

    class_name: str = field(
        default=1, metadata={"help": "func所属类名，防止不同类函数命名相同导致冲突"}
    )
    func_name: str = field(
        default=None, metadata={"help": "任务函数，由add_task添加至task_list"}
    )
    work_num: int = field(default=1, metadata={"help": "进程数量"})
    dummy: bool = field(
        default=False, metadata={"help": "False是多进程，True则是多线程"}
    )


TASK_LIST: List[Task] = []


# 添加至任务列表
def add_task(work_num: int = 1, dummy=False):
    def _add_task(func):
        TASK_LIST.append(
            Task(func.__qualname__.split(".")[0], func.__name__, work_num, dummy)
        )

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return _add_task


def clear_task():
    while TASK_LIST:
        TASK_LIST.pop()


def get_max_work_nums():
    return max([task.work_num for task in TASK_LIST])
