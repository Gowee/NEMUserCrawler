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
            result = await get(session, "https://music.163.com/user/home?id={}".format(user_id))
            d = json.loads(str_between(
                result, '<script type="application/ld+json">', '</script>'))
            user_profile = {
                'id': user_id,
                'name': d['title'],
                'avatar_url': d['images'][0] if d['images'] else "",
                'description': d['description'].rstrip("{}的最近常听、歌单、DJ节目、音乐口味、动态。".format(d['title']))
            }

            result = await post(session,
                                "https://music.163.com/weapi/user/playlist?csrf_token=",
                                headers={
                                    'Content-Type': "application/x-www-form-urlencoded"},
                                data=urlencode(nem_encrypt(
                                    {"uid": str(user_profile['id']), "wordwrap": "99", "offset": "0",
                                     "total": "true", "limit": "5", "csrf_token": ""}
                                )))
            d = json.loads(result)
            if "喜欢的音乐" not in d['playlist'][0]['name']:
                raise ValueError

            result = await get(session, "https://music.163.com/playlist?id={}".format(d['playlist'][0]['id']))
            result = str_between(result, '<ul class="f-hide">', '</ul>')
            songs = []
            for song_match in SONGS_LIST.finditer(result):
                songs.append((song_match.group('song_id'),
                              song_match.group('song_name')))
            user_profile['favorite_songs'] = songs
            return True, user_profile
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        return False, "UPSTREAM_INVALID_RESPONSE"


async def user_fetcher(request):
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

app = web.Application()
app.add_routes([web.get('/', index),
                web.get('/user/fetch/{id}', user_fetcher)])

web.run_app(app)
