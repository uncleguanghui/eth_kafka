"""
监听 web3 的区块数据
@Time    : 2021/3/26 11:23 上午
@Author  : zhangguanghui
"""
import time
import logging
from multiprocessing import Process
from collections import defaultdict
from common import w3, config
from models import Block, Transaction, Log, Receipt, Token, Contract
from utils import to_kafka, ERC20_ABI, kafka_consumer, get_latest_data

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

TOPIC_TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'


def send_block(height_or_hash):
    # 获得指定高度的区块数据并发送到 kafka
    block = w3.eth.getBlock(height_or_hash, True)
    to_kafka(Block(data=block))  # 发到 kafka
    for tx in block.transactions:
        to_kafka(Transaction(data=tx))  # 发到 kafka


def catch_up_from_start(start: int):
    # 从指定高度开始追赶，直到达到最新区块
    if not start:
        return
    current_block_index = w3.eth.blockNumber
    while current_block_index >= start:
        send_block(start)
        start += 1
        current_block_index = w3.eth.blockNumber  # 更新当前区块高度


def monitor_block():
    """
    监听 web3 区块数据和交易数据，每 0.5 秒轮询一次
    :return:
    """
    topic = config.get('kafka', 'block_topic')
    consumer = kafka_consumer(topic, group_id=f'monitor_block_{int(time.time())}')
    latest_data = get_latest_data(consumer, topic)  # kafka 的最后一条数据
    new_start = (latest_data['number'] + 1) if latest_data else None  # 已处理的最新区块高度
    catch_up_from_start(new_start)  # 追赶到最新区块高度

    # 持续更新
    block_filter = w3.eth.filter('latest')
    while True:
        for i in block_filter.get_new_entries():
            send_block(i.hex())
        time.sleep(0.5)


def monitor_logs():
    """
    监听 kafka 区块数据，出现新数据时，从 web3 获取日志
    :return:
    """
    topic = config.get('kafka', 'block_topic')
    consumer = kafka_consumer(topic, group_id='monitor_logs')
    for msg in consumer:
        block = msg.value.decode('utf-8').encode('utf-8').decode('unicode_escape')
        logs = w3.eth.filter({'fromBlock': block['number'], 'toBlock': block['number']}).get_all_entries()
        # 按交易聚合
        group = defaultdict(list)
        for log in logs:
            group[log.transactionHash].append(log)
        for g in group.values():
            to_kafka(Log(data=g))  # 发到 kafka


def monitor_receipt():
    """
    监听 kafka 交易数据，出现新数据时，从 web3 获取 receipt
    :return:
    """
    topic = config.get('kafka', 'transaction_topic')
    consumer = kafka_consumer(topic, group_id='monitor_receipt')
    for msg in consumer:
        tx = msg.value.decode('utf-8').encode('utf-8').decode('unicode_escape')
        receipt = w3.eth.getTransactionReceipt(tx['hash'])
        to_kafka(Receipt(data=receipt))  # 发到 kafka
        if receipt.get('contractAddress'):
            to_kafka(Contract(data=receipt.get('contractAddress'), block_number=receipt.get('blockNumber')))  # 发到 kafka


def monitor_token():
    """
    监听 kafka 日志数据，出现新数据时，过滤出 transfer 函数对应的日志，从 web3 获取 token
    :return:
    """
    topic = config.get('kafka', 'log_topic')
    consumer = kafka_consumer(topic, group_id='monitor_token')
    for msg in consumer:
        logs = msg.value.decode('utf-8').encode('utf-8').decode('unicode_escape')
        # 筛选出 token 地址
        addresses = set()
        for log in logs:
            if log['topics'].startswith(TOPIC_TRANSFER):
                addresses.add(log['address'])
        # 获取 token
        for address in addresses:
            contract = w3.eth.contract(address, abi=ERC20_ABI)
            block_number = logs[0]['block_number']
            to_kafka(Token(data=address, contract=contract, block_number=block_number))  # 发到 kafka


if __name__ == '__main__':
    ps = []
    for function in [
        monitor_block,
        monitor_logs,
        monitor_receipt,
        monitor_token
    ]:
        p = Process(target=function)
        p.start()
        ps.append(p)

    for p in ps:
        p.join()
