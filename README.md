# 监听以太坊数据

轮询并将结果输出到 Kafka

## 快速开始

### 1、修改配置文件

重命名配置文件：

```commandline
mv config.ini.example config.ini
```

并修改配置文件 config.ini 中的连接参数。

### 2、创建 Kafka 主题

在配置文件 config.ini 中可以看到 6 个 Kafka Topic，请创建。

### 3、准备 Python3 环境

安装 Python3，然后运行下面的命令安装相关依赖。

```commandline
pip3 install -r requirements.txt
```

### 4、运行

下面 4 个脚本分别处理不同的数据。

```commandline
python monitor_block_and_transaction.py
python monitor_log.py
python monitor_receipt_and_contract.py
python monitor_token.py
```