"""
常用工具类
@Time    : 2021/3/26 3:08 下午
@Author  : zhangguanghui
"""
import json
import logging
import collections
from kafka import KafkaProducer, KafkaConsumer
from web3.exceptions import BadFunctionCallOutput

logging.getLogger("kafka").setLevel(logging.CRITICAL)  # 隐藏 kafka 日志消息

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(filename)s[line:%(lineno)d] : %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ###################################### 特殊 ######################################
class Cache:
    def __init__(self, maxlen=3):
        self.data = collections.OrderedDict()  # 缓存已经获取过的区块哈希值（因为有的时候会得到重复值）
        self.maxlen = maxlen

    def __setitem__(self, key, value):
        # 模拟字典设置值的方法
        self.data[key] = value
        # 控制字典长度
        if len(self.data) > self.maxlen:
            self.pop()

    def pop(self):
        # 只有缓存的数据量达到要求时，才会返回最老的那条数据
        if len(self.data) >= self.maxlen:
            return self.data.popitem(last=False)[1]

    def __repr__(self):
        return str(self.data)

    def __len__(self):
        return len(self.data)


# ###################################### kafka ######################################
# 初始化 kafka 生产者
def kafka_producer(bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        api_version=(0, 10, 2, 0),
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    logger.debug(f'kafka 生产者初始化成功: {bootstrap_servers}')
    return producer


def kafka_consumer(*topics, group_id=None, bootstrap_servers='', auto_offset_reset='latest', **kwargs):
    """
    返回 kafka 消费者
    :param topics:
    :param group_id:
    :param bootstrap_servers:
    :param auto_offset_reset:
    :return:
    """
    consumer = KafkaConsumer(
        *topics,
        group_id=group_id,  # 指定此消费者实例属于的组名，可以不指定
        bootstrap_servers=bootstrap_servers,  # 指定 kafka 服务器
        auto_offset_reset=auto_offset_reset,  # 'smallest': 'earliest', 'largest': 'latest'
        value_deserializer=lambda x: json.loads(x.decode('utf-8').encode('utf-8').decode('unicode_escape')),
        **kwargs
    )
    logger.debug(f'kafka 消费者初始化成功: {bootstrap_servers}, topics: {topics}, group: {group_id}, '
                 f'auto_offset_reset: {auto_offset_reset}, 其他参数：{kwargs}')
    return consumer


# ###################################### 数据处理 ######################################

def to_normalized_address(address):
    if address is None or not isinstance(address, str):
        return address
    return address.lower()


def get_first_result(*funcs):
    for func in funcs:
        result = call_contract_function1(func)
        if result is not None:
            return result
    return None


def call_contract_function1(func):
    # BadFunctionCallOutput exception happens if the token doesn't implement a particular function
    # or was self-destructed
    # OverflowError exception happens if the return type of the function doesn't match the expected type
    result = call_contract_function2(
        func=func,
        ignore_errors=(BadFunctionCallOutput, OverflowError, ValueError),
        default_value=None)
    return clean_user_provided_content(result)


def call_contract_function2(func, ignore_errors, default_value=None):
    try:
        result = func.call()
        return result
    except Exception as ex:
        if type(ex) in ignore_errors:
            logging.error(f'An exception occurred in function {func.fn_name} of contract {func.address}. '
                          f'This exception can be safely ignored.')
        return default_value


ASCII_0 = 0


def clean_user_provided_content(content):
    if isinstance(content, str):
        # This prevents this error in BigQuery
        # Error while reading data, error message: Error detected while parsing row starting at position: 9999.
        # Error: Bad character (ASCII 0) encountered.
        return content.translate({ASCII_0: None})
    else:
        return content


ERC20_ABI = json.loads('''
[
    {
        "constant": true,
        "inputs": [],
        "name": "name",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": false,
        "inputs": [
            {
                "name": "_spender",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "approve",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": false,
        "inputs": [
            {
                "name": "_from",
                "type": "address"
            },
            {
                "name": "_to",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "transferFrom",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "decimals",
        "outputs": [
            {
                "name": "",
                "type": "uint8"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            }
        ],
        "name": "balanceOf",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "symbol",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": false,
        "inputs": [
            {
                "name": "_to",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "transfer",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            },
            {
                "name": "_spender",
                "type": "address"
            }
        ],
        "name": "allowance",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "anonymous": false,
        "inputs": [
            {
                "indexed": true,
                "name": "_from",
                "type": "address"
            },
            {
                "indexed": true,
                "name": "_to",
                "type": "address"
            },
            {
                "indexed": false,
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "Transfer",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {
                "indexed": true,
                "name": "_owner",
                "type": "address"
            },
            {
                "indexed": true,
                "name": "_spender",
                "type": "address"
            },
            {
                "indexed": false,
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "Approval",
        "type": "event"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "NAME",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "SYMBOL",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "DECIMALS",
        "outputs": [
            {
                "name": "",
                "type": "uint8"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }
]
''')
