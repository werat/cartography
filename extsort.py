import os
import sys
import heapq


BLOCK_FORMAT = '{0}_block_{1}.bl'

def _split_into_sorted_blocks(input_file, block_size):
    blocks_count = (os.stat(input_file).st_size - 1) / block_size + 1

    def key(line):
        return line[:line.index('\t')]

    blocks = []
    with open(input_file, 'r') as fi:
        for i in range(blocks_count):
            lines = [(key(line), line) for line in fi.readlines(block_size)]
            lines.sort()

            block_file = BLOCK_FORMAT.format(input_file, i)
            with open(block_file, 'w') as fo:
                for _, line in lines:
                    fo.write(line)
            blocks.append(block_file)

    return blocks

def _merge_sorted_blocks(blocks, output_file):
    def decorated_file(f):
        for line in f:
            yield line[:line.index('\t')], line

    fblocks = [decorated_file(open(f)) for f in blocks]
    with open(output_file, 'w') as fo:
        for _, line in heapq.merge(*fblocks):
            fo.write(line)

def _remove_blocks(blocks):
    for block in blocks:
        os.remove(block)


def external_sort(input_table, output_table, max_memory_usage=1024*1024*4):
    blocks = _split_into_sorted_blocks(input_table, max_memory_usage)
    _merge_sorted_blocks(blocks, output_table)
    _remove_blocks(blocks)
