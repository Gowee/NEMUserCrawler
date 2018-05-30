import json
import re
from aiohttp import web
import aiohttp
import asyncio
from urllib.parse import urlencode
from NEMUserCrawler.common.nem_crypto import encrypt as nem_encrypt


def str_between(s, left, right):
    try:
        start = s.index(left) + len(left)
        end = s.index(right, start + 1)
        return s[start:end]
    except ValueError:
        return ""


NEM_URL = "https://music.163.com"


async def get(session: aiohttp.ClientSession, *args, **kwargs):
    async with session.get(*args, **kwargs) as response:
        return await response.text()


async def post(session: aiohttp.ClientSession, *args, **kwargs):
    async with session.post(*args, **kwargs) as response:
        return await response.text()


async def index(request):
    return web.Response(text="NEM user fetcher server is up.")

SONGS_LIST = re.compile(
    r'(?<=\<li\>\<a href="\/song\?id=)(?P<song_id>\d+)(?:"\>)(?P<song_name>.+?)(?=\<\/a\>\<\/li\>)')


async def fetch_user(user_id):
    try:
        async with aiohttp.ClientSession(headers={'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}) as session:
            result = await get(session, NEM_URL + "/user/home?id={}".format(user_id))
            if '<p class="note s-fc3">很抱歉，你要查找的网页找不到</p>' in result:
                return False, "USER_NOT_EXISTING"
            d = json.loads(str_between(
                result, '<script type="application/ld+json">', '</script>'))
            user_profile = {
                'id': user_id,
                'name': d['title'],
                'avatar_url': d['images'][0] if d['images'] else "",
                'description': d['description'].rstrip("{}的最近常听、歌单、DJ节目、音乐口味、动态。".format(d['title']))
            }

            result = await post(session,
                                NEM_URL + "/weapi/user/playlist?csrf_token=",
                                headers={
                                    'Content-Type': "application/x-www-form-urlencoded"},
                                data=urlencode(nem_encrypt(
                                    {"uid": str(user_profile['id']), "wordwrap": "99", "offset": "0",
                                     "total": "true", "limit": "5", "csrf_token": ""}
                                )))
            d = json.loads(result)
            if "喜欢的音乐" not in d['playlist'][0]['name']:
                raise ValueError

            result = await get(session, NEM_URL + "/playlist?id={}".format(d['playlist'][0]['id']))
            result = str_between(result, '<ul class="f-hide">', '</ul>')
            songs = []
            for song_match in SONGS_LIST.finditer(result):
                songs.append((song_match.group('song_id'),
                              song_match.group('song_name')))
            user_profile['favorite_songs'] = songs
            return True, user_profile
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        return False, "UPSTREAM_INVALID_RESPONSE"


async def user_fetch(request):
    id = request.match_info.get('id')
    if id is None:
        return web.json_response({
            'success': False,
            'error': "INVALID_PARAM",
        })
    result = await fetch_user(id)
    if result[0] == True:
        return web.json_response({
            'success': True,
            'data': result[1]
        })
    else:
        return web.json_response({
            'success': False,
            'error': result[1]
        })


async def search_user(name, limit=100, offset=0):
    try:
        async with aiohttp.ClientSession(headers={'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}) as session:
            result = await post(session,
                                NEM_URL + "/weapi/search/get?csrf_token=",
                                headers={
                                    'Content-Type': "application/x-www-form-urlencoded"},
                                data=urlencode(nem_encrypt(
                                    {'s': name, 'limit': limit, 'csrf_token': "", 'type': 1002, 'offset': offset})))
            d = json.loads(result)
            user_profiles = []
            for user_profile in d['result']['userprofiles']:
                user_profiles.append({
                    'id': user_profile['userId'],
                    'name': user_profile['nickname'],
                    'avatar_url': user_profile['avatarUrl']
                })

            return True, {
                'total_count': d['result']['userprofileCount'],
                'users': user_profiles
            }
    except Exception as e:
        raise e
        # return False, "UPSTREAM_INVALID_RESPONSE"


async def user_search(request):
    name = request.match_info.get('name')
    limit = request.query.getone('limit', 100)
    offset = request.query.getone('offset', 0)

    if name is None:
        return web.json_response({
            'success': False,
            'error': "INVALID_PARAM",
        })
    result = await search_user(name, limit, offset)
    if result[0] == True:
        return web.json_response({
            'success': True,
            'data': result[1]
        })
    else:
        return web.json_response({
            'success': False,
            'error': result[1]
        })

app = web.Application()
app.add_routes([web.get('/', index),
                web.get('/user/fetch/{id}', user_fetch),
                web.get('/user/search/{name}', user_search)])
if __name__ == "__main__":
    import sys
    host, port = None, None
    if len(sys.argv) == 2:
        port = int(sys.argv[1])
    elif len(sys.argv) == 3:
        host = sys.argv[1]
        port = int(sys.argv[2])
    web.run_app(app, host=host, port=port)
