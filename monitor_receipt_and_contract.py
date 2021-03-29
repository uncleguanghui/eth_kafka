"""
监听 kafka 交易数据，出现新数据时，从 web3 获取 receipt
@Time    : 2021/3/28 3:43 下午
@Author  : zhangguanghui
"""
import logging
from common import w3, config
from utils import kafka_consumer
from models import Receipt, Contract
from web3.exceptions import TransactionNotFound

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(filename)s[line:%(lineno)d] : %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
    topic = config.get('kafka', 'transaction_topic')
    consumer = kafka_consumer(topic, group_id='monitor_receipt',
                              bootstrap_servers=config.get('kafka', 'bootstrap_servers'))
    last_block_height = None
    for msg in consumer:
        tx = msg.value

        current_block_height = tx["block_number"]
        if last_block_height != current_block_height:
            logger.debug(f'开始处理区块高度 {current_block_height} 下各交易的 receipt')
            last_block_height = current_block_height

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
