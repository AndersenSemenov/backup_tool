import csv
import string
import zipfile

modulus = 2 ** 32

window_size = 256
minimum_chunk_size = 512
average_expected_chunk_size = 64_000
maximum_chunk_size = 2 * average_expected_chunk_size
jump_length = average_expected_chunk_size // 2
maskC = 0x590003570000
maskJ = 0x590003560000

with open('resources/gear_table.csv') as file:
    gear_table_csv = csv.reader(file)
    gear_table = [int(s) for line in gear_table_csv for s in line]


def get_chunks_boarders(content: string, file_name):
    current = 0
    # file_chunk_list = []
    # tmp_dir = "/Users/andreysemenov/backup_tool/tmp/zero_part_zip/zero_part"

    content_size = len(content)
    i = file_name.find(".")
    remote_file_name = file_name[0:i] + "_backup_folder"
    zip_tmp_dir = f"/Users/andreysemenov/backup_tool/tmp/{remote_file_name}.zip"
    i = 0

    with zipfile.ZipFile(zip_tmp_dir, "w", zipfile.ZIP_BZIP2) as zipf:
        while current < content_size:
            right_boarder = get_chunk_boarder(content, current, content_size)

            # file_chunk_list.append(content[current:right_boarder - 1])

            # fle = open(tmp_dir + f"{i}.txt", "w+")
            # with fle:
            #     fle.write(content[current:right_boarder - 1])
            chunk_content = content[current:right_boarder - 1]
            # print(chunk_content)

            zipf.writestr(f"{i}.txt", chunk_content)
            current = right_boarder
            i += 1

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
