import logging
import sys
from logging import Formatter, StreamHandler


class LogUtil(logging.Logger):

    logging_name_to_Level = {
        "CRITICAL": logging.CRITICAL,
        "FATAL": logging.FATAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }

    def __init__(self, log_level: str, name="mylogger"):
        super(LogUtil, self).__init__(name)
        self.setLevel(self.logging_name_to_Level.get(log_level.upper(), logging.INFO))

        # 日志格式：[时间]-[日志级别]-[文件名-行号]-[信息]-[进程PID]-[线程号]
        _format = "%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] %(message)s - %(process)d-%(thread)d"

        default_handlers = {StreamHandler(sys.stdout): logging.DEBUG}
        for handler, level in default_handlers.items():
            handler.setFormatter(Formatter(_format))
            if level is not None:
                handler.setLevel(level)
            self.addHandler(handler)


logger = LogUtil(log_level="INFO")
