# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class MongoItem(scrapy.Item):
    upsert = True
    to__id = "id"

class UserProfile(MongoItem):
    collection_name = "users"

    id = scrapy.Field()
    name = scrapy.Field()
    avatar_url = scrapy.Field()
    description = scrapy.Field()
    favorite_songs = scrapy.Field()

class Song(MongoItem):
    collection_name = "songs"

    id = scrapy.Field()
    name = scrapy.Field()