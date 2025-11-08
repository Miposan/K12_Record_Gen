import os
import sys
import logging

from datetime import datetime

class _Logger(object):
    _DEFAULT_NAME = "default"
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR

    def __init__(self):
        from datatool.config import gconfig
        self.log_dir = gconfig.log_dir
        self.logger = self._configure_logging()
        # 根据入口文件路径设置日志文件
        entry_file_path = os.path.abspath(sys.argv[0])
        if "data-process/" in entry_file_path:
            self.set_logdir(tagname=entry_file_path.split(".")[0].split("data-process/")[1])

    def _configure_logging(self):
        logger = logging.getLogger("data-process")
        logger.setLevel(logging.INFO)
        # 检查是否已经有 StreamHandler
        if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
            sh = logging.StreamHandler()
            formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
            sh.setFormatter(formatter)
            logger.addHandler(sh)
            # 为控制台处理器添加标识属性
            sh.console_handler = True
        return logger

    def set_logdir(self, tagname):
        # 检查是否已经有 FileHandler
        if not any(isinstance(handler, logging.FileHandler) for handler in self.logger.handlers):
            # add log dir
            log_file = os.path.join(self.log_dir, f"{tagname}@{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            fh = logging.FileHandler(log_file, mode="w", encoding='utf-8')
            formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

    def __call__(self, print_str, level=logging.INFO, flush=True, console=True):
        console_handlers = []
        if not console:
            for handler in self.logger.handlers:
                if hasattr(handler, 'console_handler') and handler.console_handler:
                    console_handlers.append(handler)
                    self.logger.removeHandler(handler)
        
        # 记录日志
        self.logger.log(msg=print_str, level=level)
        
        # 恢复控制台处理器
        if not console:
            for handler in console_handlers:
                self.logger.addHandler(handler)
                
        if flush:
            for handler in self.logger.handlers:
                handler.flush()

log = _Logger()