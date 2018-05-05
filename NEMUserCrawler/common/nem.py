from urllib.parse import urljoin, urlsplit

BASE_URL = "http://music.163.com"
HOST = urlsplit(BASE_URL).netloc


def rel2abs(relative_url):
    """Take a relative URL and return the absolute one based on `BASE_URL`."""
    return urljoin(BASE_URL, relative_url)

def is_weapi(url):
    url = urlsplit(url)
    return url.netloc == HOST and url.path.startswith("/weapi")