# -*- coding: utf-8 -*-
import json
import re
import scrapy
import logging
from urllib.parse import urlencode
from .. import common
from ..items import UserProfile, Song


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
    or following  |    +---------+----------+
                  |              |
                  |              v
                  |      +-------+-------+
                  +------+ parse_follow  +<-+
                         +-------+-------+  |
                                 |          |
                                 +----------+
                                   if more
    """
    database_name = "nem"  # Used for TxMongoPipeline
    name = "user"
    allowed_domains = [common.nem.HOST]
    start_urls = [common.nem.rel2abs("discover")]
    custom_settings = {'DUPEFILTER_CLASS': "NEMUserCrawler.dupefilter.NemUserIDFilter",
                       'COOKIES_ENABLED':  False}

    def parse(self, response):
        for user_id in response.xpath("//a[starts-with(@href, '/user/home')]/@href").re(r"(?<=id=)\d+"):
            # print(user_id)
            yield response.follow("user/home?id={}".format(user_id), callback=self.parse_user_page)
            yield from self.request_follow(response, user_id)

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
                               priority=20)

    def parse_playlists(self, response):
        d = json.loads(response.body.decode("utf-8"))
        up: UserProfile = response.meta.get('user_profile')
        try:
            if "喜欢的音乐" not in d['playlist'][0]['name']:
                self.log("User {}({}) seems to have no Fav playlist."
                         " Name of the first playlist: {}".format(
                             up['name'], up['id'], d['playlist'][0]['name']),
                         logging.WARNING)
            #print(up['name'], d['playlist'][0]['name'])
            yield response.follow("/playlist?id={}".format(d['playlist'][0]['id']),
                                  method="GET",
                                  callback=self.parse_favorite_songs,
                                  meta={'user_profile': up,
                                        'final_stage': True},
                                  priority=30)
        except (KeyError, json.decoder.JSONDecodeError) as e:
            self.log(
                "{!r} when parsing the playlists of user {}.".format(e, up),
                logging.WARNING)
            yield self.request_playlists(response, up)  # retry
            # self.log("Error when parsing the playlists of user {}({}): .".format(
            #     up['name'], up['id']), d, logging.WARNING)

    def parse_favorite_songs(self, response):
        up: UserProfile = response.meta.get('user_profile')
        fav_songs = []
        for song in response.xpath("//ul[@class='f-hide']/li/a"):
            song_ = Song(id=int(song.xpath("@href").re("id=(.+)")
                                [0]), name=song.xpath("text()").extract_first())
            yield song_
            fav_songs.append(song_['id'])
        up['favorite_songs'] = fav_songs
        yield up

    def parse_play_histroy(self, response):
        raise NotImplementedError
        yield json.loads(response.body.decode("utf-8"))

    def request_follows(self, *args, **kwargs):
        """Requests generator for the followers and the following. (Using `yield from`.)"""
        for follow_type in ["following", "followers"]:
            # request followers and following of the user
            yield self.request_follow(follow_type, *args, **kwargs) 

    def request_follow(self, follow_type, response, user_id, offset=0, limit=100):
        """Request generator for followers or following according to `follow_type`."""
        url = {'following': "/weapi/user/getfollows/{}?csrf_token=",
               'followers': "/weapi/user/getfolloweds?csrf_token="}[follow_type].format(user_id)
        return response.follow(url,
                               method="POST",
                               headers={
                                   'Content-Type': "application/x-www-form-urlencoded"},
                               body=urlencode(common.nem.encrypt(
                                   {"userId": str(user_id), "offset": str(
                                       offset), "total": "false", "limit": str(limit), "csrf_token": ""}
                               )),
                               callback=self.parse_follow,
                               meta={'follow_user_id': user_id,
                                     'follow_offset': offset,
                                     'follow_limit': limit,
                                     'follow_type': follow_type},
                               priority=10)

    def parse_followers(self, response):
        """Only existing for backward compatibility. Use `parse_follow` instead."""
        response.meta['follow_type'] = "followers"
        response.meta['follow_user_id'] = response.meta.get('follwers_user_id')
        response.meta['follow_offset'] = response.meta.get('followers_offset')
        response.meta['follow_limit'] = response.meta.get('followers_limit')
        yield from self.parse_follow(response) 

    def parse_follow(self, response):
        d = json.loads(response.body.decode("utf-8"))
        # 'followeds' for `request_followers`, `follow` for `request_following`
        follow_type = response.meta.get("follow_type")
        key_in_data = {'followers': "followeds", 'following': 'follow'}[follow_type]
        for follower in d[key_in_data]:
            try:
                up = UserProfile(
                    id=int(follower['userId']),
                    name=follower['nickname'],
                    avatar_url=follower['avatarUrl'],
                    description=follower['signature']
                )
                yield self.request_playlists(response, up)
                yield from self.request_follows(response, up['id'])
            except KeyError as e:
                self.log("Error when parsing followers, error {}.".format(
                    e), logging.WARNING)

        if len(d[key_in_data]) == response.meta.get("followers_limit") and d['more'] is False:
            # This may happen when NEM change the restrict on their API.
            self.logger.warn(
                "The number of fetched following/ers does match against the given page size.")

        user_id = response.meta.get("follow_user_id")
        offset = len(d[key_in_data]) or response.meta.get("follow_offset")
        limit = response.meta.get("follow_limit")
        assert not (user_id is None or offset is None or limit is None)
        if d['more'] is True:
            self.log("Fetched {number} {follow_type} of {user_id}".format(number=limit,
                                                                          follow_type=follow_type,
                                                                          user_id=user_id),
                     logging.DEBUG)
            yield self.request_follow(follow_type, response, user_id, offset + limit, limit)
        else:
            self.log("Finished iterating the {} of user ({}), {} total".format(
                follow_type,
                user_id,
                offset + len(d[key_in_data])),
                logging.INFO)
