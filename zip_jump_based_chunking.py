import csv
import os.path
import string
import zipfile

import xxhash

modulus = 2 ** 32

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

window_size = window_size_8kb
minimum_chunk_size = minimum_chunk_size_8kb
average_expected_chunk_size = average_expected_chunk_size_8kb
maximum_chunk_size = 2 * average_expected_chunk_size
jump_length = average_expected_chunk_size // 2
maskC = maskC_8kb
maskJ = maskJ_8kb

with open('resources/gear_table.csv') as gear_table_file:
    gear_table_csv = csv.reader(gear_table_file)
    gear_table = [int(s) for line in gear_table_csv for s in line]


def init_chunks_and_checksums(content: string, file_name, tmp_dir):
    current = 0
    content_size = len(content)
    i = file_name.find(".")
    local_tmp_folder = os.path.join(tmp_dir, file_name[0:i])
    os.mkdir(local_tmp_folder)
    tmp_file_name = file_name[0:i] + "_backup.zip"
    zip_tmp_dir = os.path.join(local_tmp_folder, tmp_file_name)

    checksums = []
    j = 0
    with zipfile.ZipFile(zip_tmp_dir, "w", zipfile.ZIP_BZIP2) as zipf:
        while current < content_size:
            right_boarder = get_right_boarder(content, current, content_size)
            chunk_content = content[current:right_boarder]
            checksums.append(xxhash.xxh32(chunk_content).hexdigest())
            zipf.writestr(f"{j}.txt", chunk_content)
            current = right_boarder
            j += 1
    print(f"number of chunks for file - {file_name} is - {len(checksums)}")

    with open(os.path.join(local_tmp_folder, "checksums.csv"), 'w', newline='') as csv_file:
        wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        wr.writerow(checksums)
    return checksums


def calculate_checksums(content: string, file_name, tmp_dir):
    current = 0
    content_size = len(content)
    i = file_name.find(".")
    local_tmp_folder = os.path.join(tmp_dir, file_name[0:i])
    os.mkdir(local_tmp_folder)

    checksums = []
    while current < content_size:
        right_boarder = get_right_boarder(content, current, content_size)
        chunk_content = content[current:right_boarder - 1]
        checksums.append(xxhash.xxh32(chunk_content).hexdigest())
        current = right_boarder

    with open(os.path.join(local_tmp_folder, "checksums.csv"), 'w', newline='') as csv_file:
        wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        wr.writerow(checksums)
    return checksums


def get_right_boarder(content, current, content_size):
    right_boarder = get_chunk_boarder(content, current, content_size)
    if right_boarder - current < minimum_chunk_size and len(content) < current + minimum_chunk_size:
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
        return (fingerprint + gear_table[current_byte]) % modulus
    else:
        return ((fingerprint << 1) + gear_table[current_byte]) % modulus
