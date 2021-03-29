"""
监听 web3 区块数据和交易数据，每 0.5 秒轮询一次
@Time    : 2021/3/28 3:45 下午
@Author  : zhangguanghui
"""
import time
import logging
from common import w3, config
from models import Block, Transaction
from utils import kafka_consumer, Cache
from kafka.structs import TopicPartition

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(filename)s[line:%(lineno)d] : %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 遇到了相近的问题 https://ethereum.stackexchange.com/questions/87227/duplicate-events-from-get-new-entries-using-web3-py

block_cache = Cache(maxlen=3)


def send_block(height_or_hash):
    """
    获得指定高度的区块数据并发送到 kafka
    :param height_or_hash:
    :return:
    """
    # 获取指定的区块，并加入缓存列表
    block = w3.eth.getBlock(height_or_hash, True)
    block_cache[block.number] = block

    # 从缓存列表里获得待处理的区块，如果为 None，则代表缓存的数据量不够，不进行任何处理
    block = block_cache.pop()
    if block is None:
        return

    logger.debug(f'获取到高度为 {block.number} 的数据，包含交易 {len(block.transactions)} 条')
    Block(data=block).save()
    for tx in block.transactions:
        Transaction(data=tx).save()


def catch_up_from_start(start: int):
    """
    从指定高度开始追赶，直到达到最新区块
    :param start:
    :return:
    """
    if not start:
        return
    logger.debug(f'开始从区块高度 {start} 追赶')
    current_block_index = w3.eth.blockNumber
    logger.debug(f'当前最新区块高度 {current_block_index}')

    while current_block_index >= start:
        logger.debug(f'当前最新区块高度 {current_block_index}，开始处理区块高度 {start}')
        send_block(start)
        start += 1
        current_block_index = w3.eth.blockNumber  # 更新当前区块高度


def get_last_block() -> dict:
    """
    获得 kafka 的 block_topic 的最后一条数据
    :return:
    """
    topic = config.get('kafka', 'block_topic', fallback=None)
    if not topic:
        return {}
    logger.debug(f'开始检索 {topic} 里的数据')
    consumer = kafka_consumer(group_id=f'monitor_block',  # 注意不要加 topic
                              bootstrap_servers=config.get('kafka', 'bootstrap_servers'))
    partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
    last_offset_per_partition = consumer.end_offsets(partitions)
    max_partition, max_offset = sorted(last_offset_per_partition.items(), key=lambda x: x[0])[-1]
    if max_offset > 0:
        consumer.assign([max_partition])
        consumer.seek(max_partition, offset=max_offset - 1)
        msg = next(consumer)
        logger.debug(f'{topic} 最新一条数据为 {msg.value}')
        return msg.value
    return {}


def monitor_block_and_transaction():
    """
    监听 web3 区块数据和交易数据，每 0.5 秒轮询一次
    :return:
    """
    if not config.get('kafka', 'block_topic', fallback=None) or \
            not config.get('kafka', 'transaction_topic', fallback=None):
        logger.warning('config.ini 中没有 block_topic 或 transaction_topic 参数，退出 monitor_block_and_transaction 任务')
        return

    last_block = get_last_block()  # kafka 的最后一条数据
    if last_block:
        new_start = last_block['number'] + 1  # 已处理的最新区块高度
        catch_up_from_start(new_start)  # 追赶到最新区块高度
        logger.debug('追赶到最新区块高度')

    # 持续更新
    block_filter = w3.eth.filter('latest')
    # https://ethereum.stackexchange.com/questions/87227/duplicate-events-from-get-new-entries-using-web3-py
    while True:
        for i in block_filter.get_new_entries():
            send_block(i.hex())

        time.sleep(0.5)


if __name__ == '__main__':
    monitor_block_and_transaction()
