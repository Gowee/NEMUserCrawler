#!/usr/bin/env python3

# Requirement(s): pycryptodome
# Reference: https://github.com/darknessomi/musicbox/wiki/网易云音乐新登录API分析
#            https://s3.music.126.net/web/s/core.js

import json
import secrets
from string import ascii_letters, digits
from base64 import b64encode
from Crypto.Cipher import AES

__all__ = ["encrypt"]

NONCE = b'0CoJUm6Qyw8W8jud'
IV = b'0102030405060708'

MODULUS = 0x00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7
EXPONENT = 0x010001

VALID_KEY_SEQ = (ascii_letters + digits).encode("ascii")


def encrypt(data, key=None):
    if not isinstance(data, (str, bytes)):
        data = json.dumps(data)
    if not isinstance(data, bytes):
        data = data.encode("UTF-8")
    if key is None:
        # The random key must only have letters and digits..
        key = random_key(16)
    cipher_data = b64encode(aes_cbc_encrypt(
        b64encode(aes_cbc_encrypt(data, NONCE)), key))
    return {
        'params': cipher_data.decode("ascii"),
        'encSecKey': format(rsa_encrypt(key), "x").zfill(256)
    }


def random_key(size=16) -> bytes:
    return bytes(secrets.choice(VALID_KEY_SEQ) for _ in range(size))


def aes_cbc_encrypt(data, key, iv=IV) -> bytes:
    data = pad(data, 16)
    cipher = AES.new(key, AES.MODE_CBC, iv=iv)
    return cipher.encrypt(data)


def rsa_encrypt(data, exponent=EXPONENT, modulus=MODULUS) -> int:
    #data = pad(data, 126)
    data = data[::-1]
    cipher_data = square_multiply(int(data.hex(), base=16), exponent, modulus)
    return cipher_data


def pad(data_to_pad, block_size):
    if not isinstance(data_to_pad, bytes):
        raise TypeError(
            "`data_to_pad` should be `bytes` instead of {}.".format(type(data_to_pad)))
    to_pad = block_size - len(data_to_pad) % block_size
    return data_to_pad + bytes(to_pad for _ in range(to_pad))


def square_multiply(x, e, n):
    bits = []
    while e != 0:
        bits.append(e % 2)
        e //= 2

    z = 1
    for ee in reversed(bits):
        z = (z * z) % n
        if ee == 1:
            z = (z * x) % n
    return z
