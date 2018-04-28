import urllib
from .nem_crypto import encrypt

BASE_URL = "http://music.163.com"
HOST = urllib.parse.urlsplit(BASE_URL).netloc


def rel2abs(relative_url):
    """Take a relative URL and return the absolute one based on `BASE_URL`."""
    return urllib.parse.urljoin(BASE_URL, relative_url)
