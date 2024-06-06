import json
from typing import List, Union

import pandas as pd

from ._logger import logger


class JsonTool:
    """json文件读写类"""

    @classmethod
    def read(cls, file_path):
        with open(file_path, "r", encoding="utf8") as in_f:
            return json.load(in_f)

    @classmethod
    def write(cls, data: Union[list, dict], file_path):
        """_summary_

        Args:
            data (Union[list, dict]): _description_
            file_path (_type_): _description_
        """
        with open(file_path, "w", encoding="utf8") as out_f:
            json.dump(data, out_f, ensure_ascii=False)


class FileTool:
    """基础File，如txt读写操作"""

    @classmethod
    def _open(cls, file_path):
        with open(file_path, "r", encoding="utf8") as in_f:
            for line in in_f:
                item = line.strip()  # 纯文本
                yield item

    @classmethod
    def read_iter(cls, file_path):
        yield from cls._open(file_path)

    @classmethod
    def read_list(cls, file_path):
        item_list = []
        for item in cls._open(file_path):
            item_list.append(item)

        return item_list

    @classmethod
    def write(cls, item_iter, file_path):
        """写入

        Args:
            item_iter ([list or iter]): [description]
            file_path ([str]): [description]
        """
        with open(file_path, "w", encoding="utf8") as out_f:
            for item in item_iter:
                out_f.write(item + "\n")


class JsonlTool(FileTool):
    """jsonl读写操作"""

    @classmethod
    def _open(cls, file_path):
        with open(file_path, "r", encoding="utf8") as in_f:
            for line in in_f:
                try:
                    item = json.loads(line)
                except:
                    item = line.strip()  # 纯文本
                yield item

    @classmethod
    def write(cls, item_iter, file_path, mode: str = "w", buffering: int = 4096):
        """jsonl写入

        Args:
            item_iter ([list or iter]): [description]
            file_path ([str]): [description]
            mode([str]): 写入模式   a: 追加模式
            buffering([int]): 设置较小值可及时写入。 如果 buffering=0 或者 buffering=False，意味着关闭缓冲区（也就是关闭缓冲，直接写入磁盘）。
        """
        with open(file_path, mode, encoding="utf8", buffering=buffering) as out_f:
            for item in item_iter:
                out_f.write(json.dumps(item, ensure_ascii=False) + "\n")


class ExcelTool:
    """excel读写操作 不支持流式操作"""

    @classmethod
    def read_list(
        cls, file_path, sheet_name="Sheet1", todict=True, fillna: bool = True
    ):
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        if fillna:
            df = df.fillna("")
        columns = list(df.columns)
        logger.info(columns)

        lines = []
        for index, line in df.iterrows():
            if todict:
                line = cls().to_dict(line)
            lines.append(line)
        return lines

    def to_dict(self, item):
        return {k: item[k] for k in item.keys()}

    @classmethod
    def _write(
        cls, data: List[list], file_path, columns: list, sheet_name="Sheet1", width=10
    ):
        df = pd.DataFrame(data, columns=columns)

        # 设置列宽
        with pd.ExcelWriter(file_path) as writer:
            # with pd.ExcelWriter(file_path, engine='xlsxwriter', options={'strings_to_urls': False}) as writer: # 解决行数过多时url转化问题
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            worksheet = writer.sheets[sheet_name]
            [worksheet.set_column(i, i, width) for i, c in enumerate(columns)]

    @classmethod
    def write(
        cls,
        item_list: List[dict],
        file_path,
        columns=None,
        key_map: dict = None,
        width=10,
    ):
        """_summary_

        Args:
            item_list (List[dict]): 输入数据流
            file_path (_type_): 输出文件路径
            columns (_type_, optional): 字段名. Defaults to None.
            key_map (dict, optional): 字段名映射. Defaults to None.
        """
        D = []
        for item in item_list:

            if not columns:
                columns = item.keys()

            D.append([item[k] for k in columns])

        if key_map:
            columns = [key_map.get(k, k) for k in columns]

        if not columns:
            return

        cls._write(D, file_path, columns, width=width)
