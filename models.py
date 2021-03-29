"""
模型类
@Time    : 2021/3/26 3:09 下午
@Author  : zhangguanghui
"""
import logging
from common import config, producer
from utils import to_normalized_address, get_first_result

logging.getLogger("kafka").setLevel(logging.CRITICAL)  # 隐藏 kafka 日志消息
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(filename)s[line:%(lineno)d] : %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Base:
    key = ''

    def __init__(self, data):
        self.topic = config.get('kafka', self.key, fallback=None)
        self.data = data

    def parse(self) -> dict:
        pass

    def __repr__(self):
        return self.__class__.__name__

    def save(self):
        if not self.topic:
            logger.debug(f'没有找到 {self.__class__.__name__} 的主题，不写入 kafka')
            return
        elif not self.data:
            logger.debug(f'没有找到 {self.__class__.__name__} 的数据，不写入 kafka')
            return
        data = self.parse()
        future = producer.send(self.topic, value=data)
        future.add_callback(self.callback_success)
        future.add_errback(self.callback_error)

    def callback_success(self):
        logger.debug(f'{self} 发送成功')

    def callback_error(self):
        logger.error(f'{self} 发送失败')


class Block(Base):
    key = 'block_topic'

    def __repr__(self):
        # Block:(233)aaa
        return f'{self.__class__.__name__}:({self.data.get("number")})' \
               f'{self.data.get("hash").hex()}'

    def parse(self):
        return {
            'number': self.data.get('number'),
            'hash': self.data.get('hash').hex(),
            'parent_hash': self.data.get('parentHash').hex(),
            'nonce': self.data.get('nonce').hex(),
            'sha3_uncles': self.data.get('sha3Uncles').hex(),
            'logs_bloom': self.data.get('logsBloom').hex(),
            'transactions_root': self.data.get('transactionsRoot').hex(),
            'state_root': self.data.get('stateRoot').hex(),
            'receipts_root': self.data.get('receiptsRoot').hex(),
            'miner': self.data.get('miner'),
            'difficulty': self.data.get('difficulty'),
            'total_difficulty': self.data.get('totalDifficulty'),
            'size': self.data.get('size'),
            'extra_data': self.data.get('proofOfAuthorityData').hex(),
            'gas_limit': self.data.get('gasLimit'),
            'gas_used': self.data.get('gasUsed'),
            'timestamp': self.data.get('timestamp'),
            'transaction_count': len(self.data.get('transactions')),
        }


class Transaction(Base):
    key = 'transaction_topic'

    def __repr__(self):
        # Transaction:(233)aaa
        return f'{self.__class__.__name__}:({self.data.get("blockNumber")})' \
               f'{self.data.get("hash").hex()}'

    def parse(self):
        return {
            'hash': self.data.get('hash').hex(),
            'nonce': self.data.get('nonce'),
            'block_hash': self.data.get('blockHash').hex(),
            'block_number': self.data.get('blockNumber'),
            'transaction_index': self.data.get('transactionIndex'),
            'from_address': to_normalized_address(self.data.get('from')),
            'to_address': to_normalized_address(self.data.get('to')),
            'value': self.data.get('value'),
            'gas': self.data.get('gas'),
            'gas_price': self.data.get('gasPrice'),
            'input': self.data.get('input'),
        }


class Log(Base):
    key = 'log_topic'

    def __repr__(self):
        # Log:(233)aaa
        return f'{self.__class__.__name__}:({self.data[0].get("blockNumber")})' \
               f'{self.data[0].get("transactionHash").hex()}'

    def parse(self):
        # 日志的 data 类型是 list，表示一个交易的所有日志
        return [{
            'log_index': data.get('logIndex'),
            'transaction_hash': data.get('transactionHash').hex(),
            'transaction_index': data.get('transactionIndex'),
            'block_hash': data.get('blockHash').hex(),
            'block_number': data.get('blockNumber'),
            'address': to_normalized_address(data.get('address')),
            'data': data.get('data'),
            'topics': ','.join([i.hex() for i in data.get('topics')])
        } for data in self.data]


class Receipt(Base):
    key = 'receipt_topic'

    def __repr__(self):
        # Receipt:(233)aaa
        return f'{self.__class__.__name__}:({self.data.get("blockNumber")})' \
               f'{self.data.get("transactionHash").hex()}'

    def parse(self):
        return {
            'transaction_hash': self.data.get('transactionHash').hex(),
            'transaction_index': self.data.get('transactionIndex'),
            'block_hash': self.data.get('blockHash').hex(),
            'block_number': self.data.get('blockNumber'),
            'cumulative_gas_used': self.data.get('cumulativeGasUsed'),
            'gas_used': self.data.get('gasUsed'),
            'contract_address': to_normalized_address(self.data.get('contractAddress')),
            'root': self.data.get('root'),
            'status': self.data.get('status'),
        }


class Contract(Base):
    key = 'contract_topic'

    def __init__(self, *args, block_number, **kwargs):
        super().__init__(*args, **kwargs)
        self.block_number = block_number

    def __repr__(self):
        # Contract:(233)aaa
        return f'{self.__class__.__name__}:({self.block_number})' \
               f'{to_normalized_address(self.data.get("contractAddress"))}'

    def parse(self):
        return {
            'address': to_normalized_address(self.data.get('contractAddress')),
            'block_number': self.block_number
        }


class Token(Base):
    key = 'token_topic'

    def __init__(self, *args, contract, block_number, **kwargs):
        super().__init__(*args, **kwargs)
        self.block_number = block_number
        self.contract = contract

    def __repr__(self):
        # Contract:(233)aaa
        return f'{self.__class__.__name__}:({self.block_number})' \
               f'{to_normalized_address(self.data)}'

    def parse(self):
        return {
            'address': to_normalized_address(self.data),
            'symbol': get_first_result(self.contract.functions.symbol(), self.contract.functions.SYMBOL()),
            'name': get_first_result(self.contract.functions.name(), self.contract.functions.NAME()),
            'decimals': get_first_result(self.contract.functions.decimals(), self.contract.functions.DECIMALS()),
            'total_supply': get_first_result(self.contract.functions.totalSupply()),
            'block_number': self.block_number
        }
