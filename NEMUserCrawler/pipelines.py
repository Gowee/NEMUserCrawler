#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import txmongo
from pymongo.uri_parser import parse_uri
from pymongo.errors import DuplicateKeyError
from twisted.internet import defer


class TxMongoPipeline(object):
    mongo_uri = "mongodb://localhost:27017"  # default

    def __init__(self, mongo_uri, db_name, buffer_size=0):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.mongo_uri = mongo_uri or self.mongo_uri
        self.db_name = db_name or parse_uri(self.mongo_uri)['database']

        self.buffer_size = buffer_size
        if buffer_size > 0:
            raise NotImplementedError("Buffer is not implemented yet.")
            # self.buffer = {}
            # {'<collection_name>': [<item>, ...], ...}

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            db_name=crawler.settings.get('MONGO_DB'),
            buffer_size=crawler.settings.get('MONGO_BUFFER_SIZE', 0)
        )

    @defer.inlineCallbacks
    def open_spider(self, spider):
        try:
            self.db_name = spider.database_name or self.db_name
        except AttributeError:
            pass
        if self.db_name is None:
            e = "Database name for {} is not specified."\
                "It can be specified thru `MONGO_URI` or `MONGO_DB` in settings"\
                " or the `database_name` attribute of spiders".format(
                    self.__class__.__name__)
            self.logger(e)
            raise ValueError(e)
        self.connection = yield txmongo.connection.ConnectionPool(self.mongo_uri)
        self.db = self.connection[self.db_name]

    def close_spider(self, spider):
        return self.connection.disconnect()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        if self.buffer_size:
            raise NotImplementedError("Buffer is not implemented yet.")
        else:
            collection_name = item.collection_name if hasattr(item, "collection_name")\
                else item.__class__.__name__
            processed_item = dict(item)
            try:  # to use the field name specified in `to__id` as `_id` in MongoDB
                _id = processed_item.pop(item.to__id)
            except KeyError:
                _id = None
            else:
                if _id is None:
                    self.logger.warn(
                        "Item does not have the field to be used as `_id`: {}".format(item))
                processed_item['_id'] = _id
            # `upsert` here: denotes whether the insert operation is to use `insert_one` or `update` with `upsert=True`
            # in the former case, DuplicateKeyError may be raised
            if _id is None or not (hasattr(item, 'upsert') and item.upsert):
                try:
                    result = yield self.db[collection_name].insert_one(processed_item)
                except DuplicateKeyError:
                    self.logger.warn("DuplicateKeyError raised when handling {}: {}. "
                                     "Consider using `to__id` with `upsert=True`".format(collection_name, processed_item))
                else:
                    result = None
            else:
                result = yield self.db[collection_name].update({'_id': _id}, processed_item, upsert=True)
            defer.returnValue(item)
