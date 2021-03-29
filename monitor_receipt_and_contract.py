"""
监听 kafka 交易数据，出现新数据时，从 web3 获取 receipt
@Time    : 2021/3/28 3:43 下午
@Author  : zhangguanghui
"""
from logger import Logger
from config import config
from models import Receipt, Contract
from utils import kafka_consumer, get_web3
from web3.exceptions import TransactionNotFound

# 设置日志
logger = Logger(__name__, filename='receipt_contract.log')

# web3 连接
w3 = get_web3()


def monitor_receipt_and_contract():
    """
    监听 kafka 交易数据，出现新数据时，从 web3 获取 receipt
    :return:
    """
    if not config.get('kafka', 'transaction_topic', fallback=None):
        logger.warning('config.ini 中没有 transaction_topic 参数，退出 monitor_receipt_and_contract 任务')
        return
    elif not config.get('kafka', 'receipt_topic', fallback=None) or not config.get('kafka', 'contract_topic',
                                                                                   fallback=None):
        logger.warning('config.ini 中没有 receipt_topic 或 contract_topic 参数，退出 monitor_receipt_and_contract 任务')
        return
    consumer = kafka_consumer(config.get('kafka', 'transaction_topic'), group_id='monitor_receipt')
    last_block_height = None
    tx_cnt = 0  # 已处理的交易数
    for msg in consumer:
        tx = msg.value

        current_block_height = tx["block_number"]
        if last_block_height != current_block_height:
            logger.info(f'区块 {last_block_height} 共处理交易 {tx_cnt} 笔')
            logger.info(f'开始处理区块高度 {current_block_height} 下各交易的 receipt')
            last_block_height = current_block_height
            tx_cnt = 1
        else:
            tx_cnt += 1

        try:
            receipt = w3.eth.getTransactionReceipt(tx['hash'])
        except TransactionNotFound:
            logger.error(f'在区块高度 {current_block_height} 中找不到交易 {tx["hash"]}')
            continue

        Receipt(data=receipt).save()
        if receipt.get('contractAddress'):
            logger.info(f'在区块高度为 {current_block_height} 的交易 {tx["hash"]} 中'
                        f'发现新创建合约 {receipt.get("contractAddress")}')
            Contract(data=receipt, block_number=receipt.get('blockNumber')).save()


if __name__ == '__main__':
    monitor_receipt_and_contract()
