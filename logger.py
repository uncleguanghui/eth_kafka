"""
日志基础类，将日志同时输出到控制台和文件中，并按照时间自动分割日志文件。
@Time    : 2021/3/29 2:12 下午
@Author  : zhangguanghui
"""
import logging
from pathlib import Path
from config import config
from logging.handlers import TimedRotatingFileHandler


class Logger(logging.Logger):
    def __init__(self, *args, filename=None, **kwargs):
        """
        将日志同时输出到控制台和文件中，并按照时间自动分割日志文件。
        :param filename: 提示文件名，保存在 logs 目录下，如果为None，则不输出到日志
        :param level: 日志级别
        """
        # self.logger = logging.getLogger(__name__)
        super().__init__(*args, **kwargs)

        fmt = '%(asctime)s - %(levelname)s - %(filename)s[line:%(lineno)d] : %(message)s'  # 日志格式
        date_fmt = '%Y-%m-%d %H:%M:%S'  # 时间格式
        logger_formatter = logging.Formatter(fmt, date_fmt)
        logger_level = config.get('log', 'level').upper()

        # 输出到控制台
        self.add_console_handler(logger_formatter, logger_level)

        # 输出到日志文件
        if filename:
            # path = Path(inspect.stack()[1][1]).absolute()
            path = Path(__file__)
            dir_log = path.parent / 'logs'
            dir_log.mkdir(exist_ok=True)
            # 如果没有指定log路径，则在引用脚本的路径下的log文件夹内生成
            self.add_file_handler(str(dir_log / filename), logger_formatter, logger_level)

    def add_console_handler(self, logger_formatter, logger_level):
        # 输出到控制台
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logger_formatter)
        console_handler.setLevel(logger_level)
        self.addHandler(console_handler)

    def add_file_handler(self, path_file: str, logger_formatter, logger_level):
        # 输出到日志（按照天分割日志文件）,保留30份
        # when: 时间间隔：S 秒、M 分、H 小时、D 天、W 每星期（interval==0时代表星期一）、midnight 每天凌晨
        # backCount: 备份文件的个数，如果超过这个个数，就会自动删除，0不删
        file_handler = TimedRotatingFileHandler(filename=path_file, when='D', interval=1,
                                                backupCount=30, encoding='utf-8')
        file_handler.setFormatter(logger_formatter)
        file_handler.setLevel(logger_level)
        file_handler.suffix = "%Y-%m-%d.log"
        self.addHandler(file_handler)
        self.info(f'日志文件记录到 {path_file}')
