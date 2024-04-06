import csv
import os.path
import string
import zipfile

import xxhash

modulus = 2 ** 32

window_size = 256
minimum_chunk_size = 512
average_expected_chunk_size = 16_000
maximum_chunk_size = 2 * average_expected_chunk_size
jump_length = average_expected_chunk_size // 2
maskC = 0x590003570000
maskJ = 0x590003560000

with open('resources/gear_table.csv') as gear_table_file:
    gear_table_csv = csv.reader(gear_table_file)
    gear_table = [int(s) for line in gear_table_csv for s in line]


def get_chunks_boarders(content: string, file_name, tmp_dir):
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
            right_boarder = get_chunk_boarder(content, current, content_size)
            chunk_content = content[current:right_boarder - 1]
            checksums.append(xxhash.xxh32(chunk_content).hexdigest())
            zipf.writestr(f"{j}.txt", chunk_content)
            current = right_boarder
            j += 1

    with open(os.path.join(local_tmp_folder, file_name[0:i] + "_checksums"), 'w', newline='') as csv_file:
        wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        wr.writerow(checksums)
    print("successsss")


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
