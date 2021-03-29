"""

@Time    : 2021/3/28 3:44 下午
@Author  : zhangguanghui
"""
from models import Token
from logger import Logger
from config import config
from web3.exceptions import InvalidAddress
from utils import ERC20_ABI, kafka_consumer, get_web3

# 设置日志
logger = Logger(__name__, filename='token.log')
# 记录异常
# 1、topic 没有正常创建
# 2、无法处理 token 合约地址
logger_err = Logger(f'{__name__}_err', filename='err_token.log')

# web3 连接
w3 = get_web3()

TOPIC_TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'


def monitor_token():
    """
    监听 kafka 日志数据，出现新数据时，过滤出 transfer 函数对应的日志，从 web3 获取 token
    :return:
    """
    if not config.get('kafka', 'log_topic', fallback=None):
        logger_err.error('config.ini 中没有 log_topic 参数，退出 monitor_token 任务')
        return
    elif not config.get('kafka', 'token_topic', fallback=None):
        logger_err.error('config.ini 中没有 token_topic 参数，退出 monitor_token 任务')
        return
    consumer = kafka_consumer(config.get('kafka', 'log_topic'), group_id='monitor_token')
    last_block_height = None
    tx_cnt = 0  # 已处理的交易数
    for msg in consumer:
        logs = msg.value

        block_number = logs[0]['block_number']
        if last_block_height != block_number:
            logger.info(f'区块 {last_block_height} 共处理交易 {tx_cnt} 笔')
            logger.info(f'开始处理区块高度 {block_number} 下各交易的 receipt')
            last_block_height = block_number
            tx_cnt = 1
        else:
            tx_cnt += 1

        # 筛选出 token 地址
        addresses = set()
        for log in logs:
            if log['topics'].startswith(TOPIC_TRANSFER):
                addresses.add(log['address'])

        # 获取 token
        for address in addresses:
            try:
                contract = w3.eth.contract(address, abi=ERC20_ABI)
                Token(data=address, contract=contract, block_number=block_number).save()
            except InvalidAddress:
                logger_err.error(f'无法处理 token 合约地址 {address}')


if __name__ == '__main__':
    monitor_token()
