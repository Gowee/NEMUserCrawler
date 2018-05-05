from . import nem, sparse_presence_table
from . import nem_crypto as __nem_crypto
for obj in __nem_crypto.__all__:
    setattr(nem, obj, getattr(__nem_crypto, obj))