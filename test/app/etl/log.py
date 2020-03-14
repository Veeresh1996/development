
import logging

from .opts import Opts


class Log(object):
    def __init__(self, opts: Opts, logger = logging.root):
        super().__init__()
        if len(logging.root.handlers) == 0:
            logging.basicConfig()
        self.opts = opts
        self.logger = logger
        self.logger.setLevel(opts.log_level)

    def new(self, name):
        return Log(self.opts, logger=logging.getLogger(name))

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        self.logger.warn(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)
