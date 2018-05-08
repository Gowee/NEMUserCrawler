#!/usr/bin/env python3
import logging
import os
from shutil import move as fmove

from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.job import job_dir

from .common.sparse_presence_table import SparsePresenceTable, SerializationError
from .items import UserProfile


class NemUserIDFilter(BaseDupeFilter):
    def __init__(self, path=None, debug=False):
        self.file_path = None
        self.fingerprints = set()
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        self.crawled_user_ids = SparsePresenceTable(10, 2)
        if path:
            self.file_path = os.path.join(path, 'crawled_user_ids')
            if os.path.exists(self.file_path):
                with open(self.file_path, 'rb') as file:
                    self.crawled_user_ids.load_from_file(file)

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(job_dir(settings), debug)

    def request_seen(self, request):
        user_id = request.meta.get('filter_user_id')
        if user_id is None:
            # cannot recognize `request`, just allow it
            # Note: this accutually suppress `RFPDupeFilter` which may help avoid looping requests
            return False  # or None
        user_id = int(user_id)
        if request.meta.get('do_not_filter', False):
            return False
        elif request.meta.get('as_present', False):
            return self.crawled_user_ids.present(user_id)
        else:
            return self.crawled_user_ids.is_present(user_id)

    def close(self, reason):
        if self.file_path:
            if os.path.exists(self.file_path):
                fmove(self.file_path, self.file_path + ".old")
            with open(self.file_path, "wb") as file:
                self.crawled_user_ids.dump_to_file(file)

    def log(self, request, spider):
        user_id = request.meta.get('filter_user_id')
        user_profile = request.meta.get('user_profile')
        if user_profile is None:
            user_name = "UNKNOWN"
        else:
            user_name = user_profile['name']
        if self.debug:
            msg = "Skipped request associated with user: %(user_name)s(%(user_id)s)"
            self.logger.debug(msg, {'user_name': user_name, 'user_id': user_id},
                              extra={'spider': spider})
        elif self.logdupes:
            msg = ("Skipped request associated with user:: %(user_name)s(%(user_id)s)"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'user_name': user_name, 'user_id': user_id},
                              extra={'spider': spider})
            self.logdupes = False

        spider.crawler.stats.inc_value(
            'dupefilter/NemUserIDFilter/filtered', spider=spider)
