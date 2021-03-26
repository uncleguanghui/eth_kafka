"""
配置，web3，kafka
@Time    : 2021/3/26 3:25 下午
@Author  : zhangguanghui
"""
import logging
import configparser
from web3 import Web3
from pathlib import Path
from web3.middleware import geth_poa_middleware

__all__ = ['config', 'w3']

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# 读取 config.ini
path_config = Path(__file__).parent / 'config.ini'
assert path_config.exists(), '配置文件 config.ini 不存在'
config = configparser.ConfigParser()
config.read(path_config)

# 检查 ipc
path_ipc = config.get('eth', 'ipc')
assert Path(path_ipc).exists(), f'config.ini 中的 geth.ipc 文件不存在：{path_ipc}'

# 初始化 web3
w3 = Web3(Web3.IPCProvider(path_ipc, timeout=60))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)
assert w3.isConnected()
