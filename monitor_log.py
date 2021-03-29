"""
监听 kafka 区块数据，出现新数据时，从 web3 获取日志
@Time    : 2021/3/28 3:44 下午
@Author  : zhangguanghui
"""
from models import Log
from logger import Logger
from config import config
from collections import defaultdict
from utils import kafka_consumer, get_web3

# 设置日志
logger = Logger(__name__, filename='log.log')

# web3 连接
w3 = get_web3()


def monitor_logs():
    """
    监听 kafka 区块数据，出现新数据时，从 web3 获取日志
    :return:
    """
    if not config.get('kafka', 'block_topic', fallback=None):
        logger.warning('config.ini 中没有 block_topic 参数，退出 monitor_logs 任务')
        return
    elif not config.get('kafka', 'log_topic', fallback=None):
        logger.warning('config.ini 中没有 log_topic 参数，退出 monitor_logs 任务')
        return
    consumer = kafka_consumer(config.get('kafka', 'block_topic'), group_id='monitor_logs')
    for msg in consumer:
        block = msg.value
        logs = w3.eth.filter({'fromBlock': block['number'], 'toBlock': block['number']}).get_all_entries()
        # 按交易聚合
        group = defaultdict(list)
        for log in logs:
            group[log.transactionHash].append(log)
        logger.info(f'区块高度 {block["number"]} 下共获得日志 {len(logs)} 条，按交易聚合得到日志 {len(group)} 组')
        for g in group.values():
            Log(data=g).save()


if __name__ == '__main__':
    monitor_logs()
