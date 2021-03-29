"""

@Time    : 2021/3/28 3:44 下午
@Author  : zhangguanghui
"""
import logging
from models import Token
from common import w3, config
from web3.exceptions import InvalidAddress
from utils import ERC20_ABI, kafka_consumer

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(filename)s[line:%(lineno)d] : %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TOPIC_TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'


def monitor_token():
    """
    监听 kafka 日志数据，出现新数据时，过滤出 transfer 函数对应的日志，从 web3 获取 token
    :return:
    """
    if not config.get('kafka', 'log_topic', fallback=None):
        logger.warning('config.ini 中没有 log_topic 参数，退出 monitor_token 任务')
        return
    elif not config.get('kafka', 'token_topic', fallback=None):
        logger.warning('config.ini 中没有 token_topic 参数，退出 monitor_token 任务')
        return
    topic = config.get('kafka', 'log_topic')
    consumer = kafka_consumer(topic, group_id='monitor_token',
                              bootstrap_servers=config.get('kafka', 'bootstrap_servers'))
    last_block_height = None
    for msg in consumer:
        logs = msg.value

        block_number = logs[0]['block_number']
        if last_block_height != block_number:
            logger.debug(f'开始处理区块高度 {block_number} 下各交易的 receipt')
            last_block_height = block_number

        # 筛选出 token 地址
        addresses = set()
        for log in logs:
            if log['topics'].startswith(TOPIC_TRANSFER):
                addresses.add(log['address'])
        # logger.debug(f'交易 {logs[0]["transaction_hash"]} 中有 {len(addresses)} 个不同的 token 地址')

        # 获取 token
        for address in addresses:
            try:
                contract = w3.eth.contract(address, abi=ERC20_ABI)
                Token(data=address, contract=contract, block_number=block_number).save()
            except InvalidAddress:
                logger.error(f'无法处理 token 合约地址 {address}')


if __name__ == '__main__':
    monitor_token()
