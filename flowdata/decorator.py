"""
常用装饰器
"""

import functools
import os
import threading
import time
import traceback
from concurrent import futures

from ._logger import logger


def timer(info="", threshold=0.5):
    """计时器

    Args:
        info (str, optional): 辅助日志. Defaults to "".
        threshold (float, optional): 超过此值，打印warning日志. Defaults to 0.5.
    """

    def _timer(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            res = func(*args, **kwargs)

            verbose = kwargs.get("verbose", True)
            if verbose:
                if time.time() - start < threshold:
                    logger.debug(
                        f"执行函数[%s]%s，耗时 %f 秒 进程号 %s，线程号 %s",
                        func.__name__,
                        f"[info: {info}]" if info else "",
                        time.time() - start,
                        os.getpid(),
                        threading.currentThread().ident,
                    )
                else:
                    logger.warning(
                        f"执行函数[%s]%s，耗时 %f 秒 进程号 %s，线程号 %s",
                        func.__name__,
                        f"[info: {info}]" if info else "",
                        time.time() - start,
                        os.getpid(),
                        threading.current_thread().ident,
                    )
            return res

        return wrapper

    return _timer


# 超时判断 此种方式只是会跑出超时异常，但是func依然会继续执行，直到结束
# task_fn加上此装饰器，解决torch与python多进程不兼容导致卡死 ？？
def timeout(seconds=10000, works=2):
    executor = futures.ThreadPoolExecutor(works)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            future = executor.submit(func, *args, **kw)
            return future.result(timeout=seconds)

        return wrapper

    return decorator


def err_catch(info="", level="error"):
    """异常捕获装饰器

    Args:
        info (str, optional): 辅助日志. Defaults to "".
        level (str, optional): 异常信息等级. Defaults to 'error'.
    """
    level_func = logger.error if level == "error" else logger.warning

    def decorator(func):
        @functools.wraps(func)
        def decorated(*args, **kwargs):
            try:
                # print(args)  # 装饰类中函数时， self参数包含在args中
                return func(*args, **kwargs)
            except Exception as err:
                extract_list = traceback.extract_tb(err.__traceback__, limit=10)
                for item in traceback.format_list(extract_list):
                    level_func(item.strip())
                level_func(f"{info} args:{args}, kwargs: {kwargs}: {err}")

        return decorated

    return decorator


def interrupt_catch(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            pass

    return wrapper


def handle_exception(
    max_retry: int = None,
    timeout=300,
    interval=5,
    error_detail_level=1,
    is_throw_error=False,
):
    """重试机制装饰器

    Args:
        max_retry (int, optional): 重试次数. Defaults to None.
        timeout (int, optional): 超时时间. Defaults to 300.
        interval (int, optional): 每次重试间隔. Defaults to 5.
        error_detail_level (int, optional): 错误信息格式类别. Defaults to 1.
        is_throw_error (bool, optional): 重试失败时，是否抛出异常. Defaults to False.

    """
    print_success_info = "[触发重试机制-第%s次成功]: 调用方法 --> [%s]"
    print_error_info = "[触发重试机制-第%s次失败]: 调用方法 --> [%s], \n%s"

    if error_detail_level not in [0, 1, 2]:
        raise Exception("error_detail_level参数必须设置为0 、1 、2")

    def _handle_exception(func):
        @functools.wraps(func)
        def __handle_exception(*args, **kwargs):
            cnt = 0
            t0 = time.time()  # pylint: disable=C0103
            while True:
                # 达到最大重试次数或超时就退出
                if (not (max_retry is None) and cnt > max_retry) or (
                    time.time() - t0
                ) > timeout:
                    break
                try:
                    result = func(*args, **kwargs)
                    if cnt >= 1:
                        logger.warning(print_success_info, cnt, func.__name__)
                    return result

                except Exception as e:  # pylint: disable=C0103,W0703
                    error_info = ""
                    if error_detail_level == 0:
                        error_info = "错误类型是：" + str(e.__class__) + "  " + str(e)
                    elif error_detail_level == 1:
                        error_info = (
                            "错误类型是："
                            + str(e.__class__)
                            + "  "
                            + traceback.format_exc(limit=3)
                        )
                    elif error_detail_level == 2:
                        error_info = (
                            "错误类型是："
                            + str(e.__class__)
                            + "  "
                            + traceback.format_exc()
                        )

                    cnt += 1
                    if (not (max_retry is None) and cnt > max_retry) or (
                        time.time() - t0
                    ) > timeout:  # 达到超时时间，
                        logger.error(print_error_info, cnt, func.__name__, error_info)
                        if is_throw_error:  # 重新抛出错误
                            raise e
                    logger.warning(print_error_info, cnt, func.__name__, error_info)
                    time.sleep(interval)

        return __handle_exception

    return _handle_exception


def tps(step: int = 100):
    """计算数据流处理tps   直接用tqdm替代即可
        total_tps：任务处理开始总的tps
        current_tps：当前时刻的tps

    Args:
        step (int, optional): 每隔step步计算一次. Defaults to 100.
    """

    def _tps(func):
        @functools.wraps(func)
        def wrapper(instance=None, item_iter=None, **kwargs):
            def iter_fn(item_iter):
                s_time = time.time()
                s_time_2 = time.time()

                for index, item in enumerate(item_iter):
                    if index and index % step == 0:
                        total_tps = round(index / (time.time() - s_time), 3)
                        current_tps = round(step / (time.time() - s_time_2), 3)

                        s_time_2 = time.time()
                        logger.info(
                            "【 完成量: %s 】, 【 total_tps: %s 】, 【 current_tps: %s 】",
                            index,
                            total_tps,
                            current_tps,
                        )

                    yield item

            if (
                instance is None
            ):  # 装饰函数 必须关键字参数形式调用 foo(item_iter=range(10))
                return func(iter_fn(item_iter), **kwargs)
            else:  # 装饰类中函数
                return func(instance, iter_fn(item_iter), **kwargs)

        return wrapper

    return _tps
