"""
常用工具类
@Time    : 2021/3/26 3:08 下午
@Author  : zhangguanghui
"""
import json
from json import dumps
from common import config
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition
from web3.exceptions import BadFunctionCallOutput

# 初始化 kafka 生产者
kafka_producer = KafkaProducer(
    bootstrap_servers=config.get('kafka', 'bootstrap_servers').split(','),
    value_serializer=lambda x: dumps(x).encode('utf-8')
)


def kafka_consumer(topic, group_id, auto_offset_reset='latest', **kwargs):
    """
    返回消费者
    :param topic:
    :param group_id:
    :param auto_offset_reset:
    :return:
    """
    # 返回消费者
    return KafkaConsumer(
        topic,
        group_id=group_id,  # 指定此消费者实例属于的组名，可以不指定
        bootstrap_servers=config.get('kafka', 'bootstrap_servers').split(','),  # 指定 kafka 服务器
        auto_offset_reset=auto_offset_reset,  # 'smallest': 'earliest', 'largest': 'latest'
        **kwargs
    )


def topic_has_data(consumer, topic) -> bool:
    partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
    last_offset_per_partition = consumer.end_offsets(partitions)
    return any(last_offset_per_partition.values())


def get_latest_data(consumer, topic):
    if topic_has_data(consumer, topic):
        for msg in consumer:
            msg_new = msg.value.decode('utf-8').encode('utf-8').decode('unicode_escape')
            return msg_new


def to_kafka(obj):
    if not obj.topic or not obj.data:
        return
    data = obj.parse()
    kafka_producer.send(obj.topic, value=data)


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
            print('An exception occurred in function {} of contract {}. '.format(func.fn_name, func.address)
                  + 'This exception can be safely ignored.')
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
