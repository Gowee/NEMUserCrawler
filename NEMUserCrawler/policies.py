from scrapy.exceptions import IgnoreRequest
from .common.nem import is_weapi

class NEMWeAPIBanDetectionPolicy(object):
    """Ban dection rule for weapi of NEM."""

    def response_is_ban(self, request, response):
        return response.status == 503 and b"nginx" in response.body 

    def exception_is_ban(self, request, exception):
        return False
