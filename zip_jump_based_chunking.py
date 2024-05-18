import csv
import string
import constants

modulus = 2**32

window_size_8kb = 256
minimum_chunk_size_8kb = 1_024
average_expected_chunk_size_8kb = 8_192
maximum_chunk_size_8kb = 2 * average_expected_chunk_size_8kb
jump_length_8kb = average_expected_chunk_size_8kb // 2
maskC_8kb = 0x590003570000
maskJ_8kb = 0x590003560000

window_size_16kb = 512
minimum_chunk_size_16kb = 2_048
average_expected_chunk_size_16kb = 16_384
maximum_chunk_size_16kb = 2 * average_expected_chunk_size_16kb
jump_length_16kb = average_expected_chunk_size_16kb // 2
maskC_16kb = 0x590013570000
maskJ_16kb = 0x590013560000

window_size_64kb = 2_048
minimum_chunk_size_64kb = 8_192
average_expected_chunk_size_64kb = 65_536
maximum_chunk_size_64kb = 2 * average_expected_chunk_size_64kb
jump_length_64kb = average_expected_chunk_size_64kb // 2
maskC_64kb = 0x599813570000
maskJ_64kb = 0x599813560000

window_size = window_size_16kb
minimum_chunk_size = minimum_chunk_size_16kb
average_expected_chunk_size = average_expected_chunk_size_16kb
maximum_chunk_size = 2 * average_expected_chunk_size
jump_length = average_expected_chunk_size // 2
maskC = maskC_16kb
maskJ = maskJ_16kb

with open(constants.GEAR_ARRAY_LOCATION) as gear_array_file:
    gear_array_csv = csv.reader(gear_array_file)
    gear_array = [int(s) for line in gear_array_csv for s in line]


def get_right_boarder(content, current, content_size):
    right_boarder = get_chunk_boarder(content, current, content_size)
    if (
        right_boarder - current < minimum_chunk_size
        and len(content) < current + minimum_chunk_size
    ):
        right_boarder = current + minimum_chunk_size - 1
    elif right_boarder - current > maximum_chunk_size:
        right_boarder = current + maximum_chunk_size - 1
    return right_boarder


def get_chunk_boarder(content: string, current: int, content_size: int) -> int:
    i = current
    fingerprint = 0
    curr_window_size = 0
    while i < content_size:
        ord_val = ord(content[i])
        if ord_val > 256:
            ord_val = 34
        fingerprint = gear_consume(fingerprint, ord_val, curr_window_size)

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
        return (fingerprint + gear_array[current_byte]) % modulus
    else:
        return ((fingerprint << 1) + gear_array[current_byte]) % modulus
