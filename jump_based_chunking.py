import random
import string

table = [random.randint(0, 2 ** 32 - 1)] * 256
modulus = 2 ** 32  #

window_size = 256
minimum_chunk_size = 512
average_expected_chunk_size = 8192
maximum_chunk_size = 2 * average_expected_chunk_size
jump_length = average_expected_chunk_size / 2
maskC = 0x590003570000
maskJ = 0x590003560000


def get_chunks_boarders(content: string) -> list:
    current = 0
    file_chunk_list = []
    content_size = len(content)
    while current < content_size:
        right_boarder = get_chunk_boarder(content, current, content_size)
        file_chunk_list.append(string[current:current + right_boarder])
        current = right_boarder
    return file_chunk_list


def get_chunk_boarder(content: string, current: int, content_size: int) -> int:
    i = current
    fingerprint = 0
    curr_window_size = 0
    while i < content_size:
        fingerprint = slide_window_on_one_byte(fingerprint, ord(content[i]), window_size)
        if curr_window_size == window_size and fingerprint & maskJ == 0:
            if fingerprint & maskC == 0:
                return i
            fingerprint = 0
            curr_window_size = 0
            i += jump_length
        i += 1

    return min(i, content_size)


def slide_window_on_one_byte(fingerprint, current_byte, curr_window_size) -> int:
    if curr_window_size < window_size:
        return gear_consume(fingerprint, current_byte, False)
    else:
        return gear_consume(fingerprint, current_byte, True)


def gear_consume(fingerprint, current_byte, is_proper_window_size: bool) -> int:
    if is_proper_window_size:
        return ((fingerprint << 1) + table[current_byte]) % modulus
    else:
        return (fingerprint + table[current_byte]) % modulus
