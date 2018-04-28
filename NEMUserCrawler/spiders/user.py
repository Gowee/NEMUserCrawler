# -*- coding: utf-8 -*-
import json
import re
import scrapy
import logging
from urllib.parse import urlencode
from .. import common
from ..items import UserProfile


class UserSpider(scrapy.Spider):
    """
 +----------------+      +---------------+
 |parse (discover)+----->+parse_user_page|
 +----------------+      +-------+-------+
                                 |
                                 v
                         +-------+-------+
                  +----->+parse_playlists|
                  |      +-------+-------+
                  |              |
                  |              v
                  |    +---------+----------+
for each follower |    |parse_favorite_songs|
                  |    +---------+----------+
                  |              |
                  |              v
                  |      +-------+-------+
                  +------+parse_followers+<-+
                         +-------+-------+  |
                                 |          |
                                 +----------+
                                   if more
    """
    name = "user"
    allowed_domains = [common.nem.HOST]
    start_urls = [common.nem.rel2abs("discover")]
    custom_settings = {'DUPEFILTER_CLASS': "NEMUserCrawler.dupefilter.NemUserIDFilter"
                       ''}

    def parse(self, response):
        for user_id in response.xpath("//a[starts-with(@href, '/user/home')]/@href").re(r"(?<=id=)\d+"):
            # print(user_id)
            yield response.follow("user/home?id={}".format(user_id), callback=self.parse_user_page)
            yield self.request_followers(response, user_id)

    REGEX_USER_ID = re.compile(r".+user/home\?id=(?P<id>\d+)")

    def parse_user_page(self, response):
        d = response.css(
            'script[type="application/ld+json"]::text').extract_first()
        d = json.loads(d)
        user_id = self.REGEX_USER_ID.match(d['@id']).group('id')
        up = UserProfile(
            id=int(user_id),
            name=d['title'],
            avatar_url=d['images'][0] if len(d['images']) > 0 else "",
            description=d['description']
        )
        yield self.request_playlists(response, up)

    def request_play_history(self):
        raise NotImplementedError
        # yield scrapy.Request(nem.common.rel2abs("weapi/v1/play/record?csrf_token="),
        #                      method="POST",
        #                      headers={'Content-Type': "application/x-www-form-urlencoded"},
        #                      body=urlencode(nem.crypto.encrypt(
        #                          {"uid": str(user_id), "type": "-1", "limit": "1000", "offset": "0", "total": "true",
        #                           "csrf_token": ""})),
        #                      callback=self.parse_play_histroy)

    def request_playlists(self, response, user_profile):
        return response.follow("/weapi/user/playlist?csrf_token=",
                               method="POST",
                               headers={
                                   'Content-Type': "application/x-www-form-urlencoded"},
                               body=urlencode(common.nem.encrypt(
                                   {"uid": str(user_profile['id']), "wordwrap": "99", "offset": "0",
                                    "total": "true", "limit": "5", "csrf_token": ""}
                               )),
                               callback=self.parse_playlists,
                               meta={'user_profile': user_profile},
                               priority=2)

    def parse_playlists(self, response):
        d = json.loads(response.body.decode("utf-8"))
        up: UserProfile = response.meta.get('user_profile')
        try:
            if "喜欢的音乐" not in d['playlist'][0]['name']:
                self.log("User {}({}) seems to have no Fav playlist. Name of the first playlist: {}".format(up.name, up.id, d['playlist'][0]['name']),
                         logging.WARNING)
            #print(up['name'], d['playlist'][0]['name'])
            yield response.follow("/playlist?id={}".format(d['playlist'][0]['id']),
                                  method="GET",
                                  callback=self.parse_favorite_songs,
                                  meta={'user_profile': up, 'final_stage': True},
                                  priority=3)
        except KeyError:
            self.log("Error when parsing the playlists of user {}({}).".format(
                up.name, up.id), logging.WARNING)

    def parse_favorite_songs(self, response):
        up: UserProfile = response.meta.get('user_profile')
        fav_songs = []
        for song in response.xpath("//ul[@class='f-hide']/li/a"):
            fav_songs.append((song.xpath("@href").re("id=(.+)")
                              [0], song.xpath("text()").extract_first()))
        up['favorite_songs'] = fav_songs
        yield up

    def parse_play_histroy(self, response):
        raise NotImplementedError
        yield json.loads(response.body.decode("utf-8"))

    def request_followers(self, response, user_id, offset=0, limit=100):
        return response.follow("/weapi/user/getfolloweds?csrf_token=",
                               method="POST",
                               headers={
                                   'Content-Type': "application/x-www-form-urlencoded"},
                               body=urlencode(common.nem.encrypt(
                                   {"userId": user_id, "offset": str(
                                       offset), "total": "false", "limit": str(limit), "csrf_token": ""}
                               )),
                               callback=self.parse_followers,
                               meta={"followers_user_id": user_id,
                                     "followers_offset": offset,
                                     "followers_limit": limit},
                               priority=1)

    def parse_followers(self, response):
        d = json.loads(response.body.decode("utf-8"))
        if d['code'] != 200:
            self.log(
                "Error when parsing followers, error code: {}.".format(d['code']))
            return
        for follower in d['followeds']:
            try:
                up = UserProfile(
                    id=int(follower['userId']),
                    name=follower['nickname'],
                    avatar_url=follower['avatarUrl'],
                    description=follower['signature']
                )
                yield self.request_playlists(response, up)
            except KeyError as e:
                self.log("Error when parsing followers, error {}.".format(
                    e), logging.WARNING)

        user_id = response.meta.get("followers_user_id")
        offset = response.meta.get("followers_offset")
        limit = response.meta.get("followers_limit")
        if d['more'] is True:
            yield self.request_followers(response, user_id, offset + limit, limit)
        else:
            self.log("Finished iterating the followers of user ({}), {} total".format(
                user_id,
                offset + len(follower)),
                logging.INFO)
