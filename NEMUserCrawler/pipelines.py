#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import txmongo
from pymongo.uri_parser import parse_uri
from pymongo.errors import DuplicateKeyError, BulkWriteError
from twisted.internet import defer
from scrapy.exceptions import NotConfigured
from pymongo import InsertOne, UpdateOne


class TxMongoPipeline(object):
    mongo_uri = "mongodb://localhost:27017"  # default

    def __init__(self, mongo_uri, db_name, buffer_size=0):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.mongo_uri = mongo_uri or self.mongo_uri
        self.db_name = db_name or parse_uri(self.mongo_uri)['database']

        self.buffer_size = buffer_size
        if buffer_size > 0:
            self.buffer = {}
            self.buffer_count = 0

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            db_name=crawler.settings.get('MONGO_DB'),
            buffer_size=crawler.settings.getint('MONGO_BUFFER_SIZE', 0)
        )

    @defer.inlineCallbacks
    def open_spider(self, spider):
        try:
            # TODO: get db_name from `spider.name`
            self.db_name = spider.database_name or self.db_name
        except AttributeError:
            pass
        if self.db_name is None:
            e = "Database name for {} is not specified."\
                "It can be specified thru `MONGO_URI` or `MONGO_DB` in settings"\
                " or the `database_name` attribute of spiders".format(
                    self.__class__.__name__)
            self.logger.error(e)
            raise NotConfigured(e)
        self.logger.info("TxMongoPipeline activated, uri: {}, database: {}, buffer size: {}.".format(self.mongo_uri, self.db_name, self.buffer_size))
        self.connection = yield txmongo.connection.ConnectionPool(self.mongo_uri)
        self.db = self.connection[self.db_name]

    @defer.inlineCallbacks
    def close_spider(self, spider):
        if self.buffer:
            yield self.flush_buffer()
        if self.connection:
            yield self.connection.disconnect()

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        collection_name = item.collection_name if hasattr(item, "collection_name")\
            else item.__class__.__name__
        processed_item = dict(item)
        try:  # to use the field name specified in `to__id` as `_id` in MongoDB
            _id = processed_item.pop(item.to__id)
        except KeyError:
            # `to__id` not specified
            _id = None
        else:
            if _id is None:
                # `to__id` specified, but the field specified by it not existing
                self.logger.warn(
                    "Item does not have the field to be used as `_id`: {}".format(item))
            processed_item['_id'] = _id
            # Now, the name of the field specified by `item.to__id` is replaced by the value of `item.to__id`.

        # `upsert` here: denotes whether the insert operation is to use `insert_one` or `update` with `upsert=True`
        # in the former case, DuplicateKeyError may be raised
        upsert = _id is not None and hasattr(item, 'upsert') and item.upsert
        # TODO: test error handling
        if self.buffer_size:
            # buffer enabled
            if self.buffer_count >= self.buffer_size:
                result = yield self.flush_buffer()
            else:
                operation = UpdateOne({'_id': _id}, {'$set': processed_item}, upsert=True) if upsert else InsertOne(processed_item)
                self.buffer.setdefault(collection_name, []).append(operation)
                self.buffer_count += 1
        else:
            # buffer disabled
            if upsert:
                result = yield self.db[collection_name].update({'_id': _id}, processed_item, upsert=True)
            else:
                try:
                    result = yield self.db[collection_name].insert_one(processed_item)
                except DuplicateKeyError as e:
                    self.logger.warn("{!r} raised when handling {}: {}. "
                                     "Consider using `to__id` with `upsert=True`".format(e, collection_name, processed_item))
                    result = e
        spider.crawler.stats.inc_value(
            'pipeline/txmongo/{}'.format(collection_name), spider=spider)
        defer.returnValue(item)

    @defer.inlineCallbacks
    def flush_buffer(self):
        results = []
        buffer = self.buffer.copy() # execution flow switched to other coroutines when bulk write
        self.buffer.clear()
        self.buffer_count = 0
        for collection_name, operations in buffer.items():
            try:
                result = yield self.db[collection_name].bulk_write(operations, ordered=False)
                self.logger.debug("Buffer flushed, {} for collection {}: {}".format(len(operations), collection_name, result.bulk_api_result))
            except BulkWriteError as e:
                self.logger.error("{!r} when writing buffer: {}".format(e, e.details))
                result = e.details
                results.append(result)
        defer.returnValue(results)