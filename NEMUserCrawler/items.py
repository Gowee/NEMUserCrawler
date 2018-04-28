# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class UserProfile(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    avatar_url = scrapy.Field()
    description = scrapy.Field()
    favorite_songs = scrapy.Field()
