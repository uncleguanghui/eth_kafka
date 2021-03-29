"""
配置，web3，kafka
@Time    : 2021/3/26 3:25 下午
@Author  : zhangguanghui
"""
import logging
import configparser
from web3 import Web3
from pathlib import Path
from utils import kafka_producer
from web3.middleware import geth_poa_middleware

__all__ = ['config', 'w3', 'producer']

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(filename)s[line:%(lineno)d] : %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 读取配置文件
path_config = Path(__file__).parent / 'config.ini'
assert path_config.exists(), '配置文件 config.ini 不存在'
config = configparser.ConfigParser()
config.read(path_config)

# 初始化 web3，按一定优先级遍历参数
w3 = None
for key, provider, kwargs in [
    ('ipc', Web3.IPCProvider, {'timeout': 30}),
    ('ws', Web3.WebsocketProvider, {'websocket_timeout': 30}),
    ('http', Web3.HTTPProvider, {}),
]:
    url = config.get('eth', key, fallback=None)
    error_info = "尝试下一种连接方式" if key != "http" else "所有尝试都失败"
    if url:
        w3 = Web3(provider(url, **kwargs))
        if w3.isConnected():
            logger.info(f'以 {key} 方式成功连接 web3')
            break
        logger.warning(f'以 {key} 方式连接 web3 失败，{error_info}')
    else:
        logger.warning(f'config.ini 中没有 {key} 参数，{error_info}')
if not w3 or not w3.isConnected():
    raise ValueError('请指定正确的 web3 连接方式')
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

# 连接 kafka
producer = kafka_producer(bootstrap_servers=config.get('kafka', 'bootstrap_servers'))
