"""
监听 kafka 区块数据，出现新数据时，从 web3 获取日志
@Time    : 2021/3/28 3:44 下午
@Author  : zhangguanghui
"""
import logging
from models import Log
from common import w3, config
from utils import kafka_consumer
from collections import defaultdict

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(filename)s[line:%(lineno)d] : %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
    consumer = kafka_consumer(config.get('kafka', 'block_topic'), group_id='monitor_logs',
                              bootstrap_servers=config.get('kafka', 'bootstrap_servers'))
    for msg in consumer:
        block = msg.value
        logs = w3.eth.filter({'fromBlock': block['number'], 'toBlock': block['number']}).get_all_entries()
        logger.debug(f'区块高度 {block["number"]} 下共获得日志 {len(logs)} 条')
        # 按交易聚合
        group = defaultdict(list)
        for log in logs:
            group[log.transactionHash].append(log)
        logger.debug(f'区块高度 {block["number"]} 按交易聚合得到日志 {len(group)} 组')
        for g in group.values():
            Log(data=g).save()


if __name__ == '__main__':
    monitor_logs()
