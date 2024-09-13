from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

NAME = "flowdata"
VERSION = "0.1.6"
DESCRIPTION = "流式数据简易处理包"
EMAIL = "zouweidong72@gmail.com"
AUTHOR = "zouweidong"


setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=EMAIL,
    packages=find_packages(exclude=["tests", "tests.*"]),
    description="流式数据简易处理包",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/zouweidong91/flowdata",
    python_requires=">=3.6",
    install_requires=["pandas", "XlsxWriter>=3.0.3", "tqdm"],
)
