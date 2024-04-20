from setuptools import setup, find_packages


NAME = 'flowdata'
VERSION = '0.1.0'
DESCRIPTION = '流式数据简易处理包'
EMAIL = 'zouweidong72@gmail.com'
AUTHOR = 'zouweidong'


setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=EMAIL,
    packages=find_packages(
        exclude=['tests', 'tests.*']
    ),
    python_requires='>=3.6',
    install_requires=[
    'pandas'
],
)