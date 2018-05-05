import logging

from twisted.internet import task

from scrapy.exceptions import NotConfigured
from scrapy.extensions.logstats import LogStats 
from scrapy import signals

logger = logging.getLogger(__name__)


class TxMongoLogStats(LogStats):
    """Log TxMongoPipeline stats periodically"""

    def __init__(self, stats, interval=60.0):
        super().__init__(stats=stats, interval=interval)
        self.prev_stats = {}

    PREFIX = "pipeline/txmongo/"
    def log(self, spider):
        for k, v in self.stats.get_stats().items():
            if k.startswith(self.PREFIX):
                rate = (v - self.prev_stats.get(k, 0)) * self.multiplier
                self.prev_stats[k] = v

                msg = "TxMongoPipeline stored %(amount)d %(name)s (at %(rate)d /min)"
                log_args = {'amount': v, 'name': k[len(self.PREFIX):], 'rate': rate}
                logger.info(msg, log_args, extra={'spider': spider})
