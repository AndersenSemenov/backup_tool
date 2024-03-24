import csv
import string

modulus = 2 ** 32

window_size = 256
minimum_chunk_size = 512
average_expected_chunk_size = 8192
maximum_chunk_size = 2 * average_expected_chunk_size
jump_length = average_expected_chunk_size // 2
maskC = 0x590003570000
maskJ = 0x590003560000

with open('resources/gear_table.csv') as file:
    gear_table_csv = csv.reader(file)
    gear_table = [int(s) for line in gear_table_csv for s in line]


def get_chunks_boarders(content: string) -> list:
    current = 0
    file_chunk_list = []
    content_size = len(content)
    while current < content_size:
        right_boarder = get_chunk_boarder(content, current, content_size)
        file_chunk_list.append(content[current:right_boarder - 1])
        current = right_boarder
    return file_chunk_list


def get_chunk_boarder(content: string, current: int, content_size: int) -> int:
    i = current
    fingerprint = 0
    curr_window_size = 0
    while i < content_size:
        fingerprint = gear_consume(fingerprint, ord(content[i]), curr_window_size)

        if curr_window_size < window_size:
            curr_window_size += 1

        if curr_window_size == window_size and fingerprint & maskJ == 0:
            if fingerprint & maskC == 0:
                return i
            fingerprint = 0
            curr_window_size = 0
            i += jump_length
        i += 1

    return min(i, content_size)


def gear_consume(fingerprint, current_byte, curr_window_size) -> int:
    if curr_window_size < window_size:
        return (fingerprint + gear_table[current_byte]) % modulus
    else:
        return ((fingerprint << 1) + gear_table[current_byte]) % modulus
