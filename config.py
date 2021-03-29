"""
配置，web3，kafka
@Time    : 2021/3/26 3:25 下午
@Author  : zhangguanghui
"""
import configparser
from pathlib import Path

# 读取配置文件
path_config = Path(__file__).parent / 'config.ini'
assert path_config.exists(), '配置文件 config.ini 不存在'
config = configparser.ConfigParser()
config.read(path_config)
