import functools
import json
import traceback
import signal

from dart.context.database import db


class Worker(object):
    def __init__(self, tool, logger):
        self.tool = tool
        self.logger = logger

    def run(self):
        signal_received = Wrapper(False)
        partial = functools.partial(_handler, self.logger, signal_received)

        # try to allow for graceful shutdown
        for sig in [signal.SIGTERM, signal.SIGINT]:
            signal.signal(sig, partial)

        self.logger.info('started worker tool: %s' % type(self.tool).__name__)
        while not signal_received.value:
            try:
                self.tool.run()

            except Exception:
                self.logger.error(json.dumps(traceback.format_exc()))

            finally:
                db.session.rollback()


class Wrapper(object):
    def __init__(self, value):
        self.value = value


# noinspection PyUnusedLocal
def _handler(logger, signal_received, signum, frame):
    logger.info('signal received: %s' % signum)
    signal_received.value = True
