#!/usr/bin/env python3
from math import ceil
import struct

__all__ = ["SparsePresenceTable", "SerializationError"]


class SerializationError(Exception):
    pass


class SparsePresenceTable():
    """Used to record whether numbers in a prescribed range has been present or not by bits in `bytearray`."""

    def __init__(self, length, sparse_prefix_length, prefix_to_preallocate=None):
        self._block_amount = 10 ** sparse_prefix_length
        # in bit
        self._block_effective_size = 10 ** (length - sparse_prefix_length)
        # in byte, possible trivial internal fragmentation counted
        self._block_size = ceil(self._block_effective_size / 8)
        self._table = [None for _ in range(self._block_amount)]
        if prefix_to_preallocate:
            for prefix in prefix_to_preallocate:
                self._table[prefix] = bytearray(self._block_size)

    def get_block(self, prefix):
        return self._table[prefix]

    def is_present(self, no):
        block, index_in_block = divmod(no, self._block_effective_size)
        index_of_byte, index_in_byte = divmod(index_in_block, 8)
        if self._table[block] is None:
            return False
        return self._table[block][index_of_byte] & (0b1 << (7 - index_in_byte)) > 0

    def set_present(self, no):
        block, index_in_block = divmod(no, self._block_effective_size)
        index_of_byte, index_in_byte = divmod(index_in_block, 8)
        if self._table[block] is None:
            self._table[block] = bytearray(self._block_size)
        self._table[block][index_of_byte] = self._table[block][index_of_byte] | (
            0b1 << (7 - index_in_byte))

    def present(self, no):
        block, index_in_block = divmod(no, self._block_effective_size)
        index_of_byte, index_in_byte = divmod(index_in_block, 8)
        if self._table[block] is None:
            result = False
            self._table[block] = bytearray(self._block_size)
        else:
            result = self._table[block][index_of_byte] & (
                0b1 << (7 - index_in_byte)) > 0
        self._table[block][index_of_byte] = self._table[block][index_of_byte] | (
            0b1 << (7 - index_in_byte))
        return result

    def __contains__(self, item):
        return self.is_present(item)

    def dump_to_file(self, file):
        def pack_into_file(fmt, *args):
            temp = struct.pack(fmt, *args)
            return file.write(temp)
        pack_into_file("!HL", self._block_amount, self._block_size)
        byte_count = 0
        for block in self._table:
            if block is None:
                pack_into_file("!?", False)
            else:
                pack_into_file("!?", True)
                byte_count += file.write(block)
        return byte_count

    def load_from_file(self, file):
        def unpack_from_file(fmt):
            temp = file.read(struct.calcsize(fmt))
            if len(temp) != struct.calcsize(fmt):
                raise SerializationError(
                    "file ended unexpectedly when unpack {}".format(fmt))
            return struct.unpack(fmt, temp)
        block_amount, block_size = unpack_from_file("!HL")
        if block_amount != self._block_amount or block_size != self._block_size:
            raise SerializationError("`block_amount` and/or `block_size` mismatch: expect {}/{}, got {}/{}".format(
                self._block_amount,
                self._block_size,
                block_amount,
                block_size))
        byte_count = 0
        for i in range(block_amount):
            vacant = unpack_from_file("!?")
            if vacant:
                continue
            else:
                block = bytearray(file.read(self._block_size))
                if len(block) != self._block_size:
                    raise SerializationError("file too small: at block {}, got {}/{} bytes".format(
                        i,
                        len(block),
                        self._block_size))
                byte_count += len(block)
                self._table[i] = bytearray(block)
        return byte_count
