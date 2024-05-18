import csv
import os
import shutil
import string
import zipfile
import constants
from io import StringIO

import xxhash

from full_backup_processor import create_chunked_file
from ssh_client import RemoteConnectionClient
from zip_jump_based_chunking import get_right_boarder


def process_incremental_backup(
    hostname: string,
    username: string,
    private_key_file_path: string,
    local_path: string,
    remote_path: string,
    tmp_dir: string,
):
    os.mkdir(tmp_dir)

    client = RemoteConnectionClient(hostname, username, private_key_file_path)
    remote_hashes_dict = {}
    recursive_remote_files_init_dir_walk(remote_path, client, remote_hashes_dict)
    print(remote_hashes_dict)

    for root, subdirs, files in os.walk(local_path):
        for local_file in files:
            index_of_file_extension = local_file.find(".")
            local_file_name = (
                local_file[0:index_of_file_extension]
                + "_"
                + local_file[index_of_file_extension + 1 :]
            )
            local_tmp_folder = os.path.join(
                tmp_dir, get_relative_root_path(local_path, root), local_file_name
            )

            if local_file_name in remote_hashes_dict:
                _, stdout, _ = client.ssh_client.exec_command(
                    f"cd {os.path.join(remote_path, get_relative_root_path(local_path, root), local_file_name)} && ls -1 | wc -l"
                )
                version = stdout.read().decode("utf-8")
                last_version_number = int(version[0: version.find("\n")]) + 1

                remote_checksums = remote_hashes_dict[local_file_name]
                local_checksums = []

                diff_chunks = {}

                with open(os.path.join(root, local_file)) as local_file_stream:
                    content = local_file_stream.read()
                    content_size = len(content)
                    os.mkdir(local_tmp_folder)

                    j = 0
                    current = 0
                    while current < content_size:
                        right_boarder = get_right_boarder(
                            content, current, content_size
                        )
                        chunk_content = content[current:right_boarder]
                        local_checksums.append(xxhash.xxh32(chunk_content).hexdigest())
                        current = right_boarder
                        j += 1

                    print(f"FILE - {local_file_name}")
                    print(f"local - {local_checksums}")
                    print(f"remote - {remote_checksums}")

                    deduplicated_chunks = []
                    j = 0
                    boarder_j = 0
                    current = 0
                    while (
                        current < content_size
                        and j < len(remote_checksums)
                        and j < len(local_checksums)
                    ):
                        right_boarder = get_right_boarder(
                            content, current, content_size
                        )
                        chunk_content = content[current:right_boarder]

                        if j >= boarder_j and local_checksums[j] != remote_checksums[j]:
                            left_remote_pos = find_remote_checksum_position(
                                remote_checksums, local_checksums[j]
                            )
                            right_remote_pos = find_right_remote_dedup_boarder(
                                local_checksums, remote_checksums, j, left_remote_pos
                            )

                            if left_remote_pos == -1:
                                diff_chunks[j] = chunk_content
                                boarder_j += 1
                            else:
                                print(
                                    f"left_val - {remote_checksums[left_remote_pos]}, "
                                    f"right_val - {remote_checksums[right_remote_pos]}"
                                )
                                deduplicated_chunks.append(
                                    DedupReference(
                                        j,
                                        j + (right_remote_pos - left_remote_pos),
                                        left_remote_pos,
                                        right_remote_pos,
                                    )
                                )
                                boarder_j = j + (right_remote_pos - left_remote_pos) + 1

                        current = right_boarder
                        j += 1

                    while current < content_size and j < len(local_checksums):
                        right_boarder = get_right_boarder(
                            content, current, content_size
                        )
                        chunk_content = content[current:right_boarder]

                        if j >= boarder_j:
                            left_remote_pos = find_remote_checksum_position(
                                remote_checksums, local_checksums[j]
                            )
                            right_remote_pos = find_right_remote_dedup_boarder(
                                local_checksums, remote_checksums, j, left_remote_pos
                            )

                            if left_remote_pos == -1:
                                diff_chunks[j] = chunk_content
                                boarder_j += 1
                            else:
                                print(
                                    f"left_val - {remote_checksums[left_remote_pos]}, "
                                    f"right_val - {remote_checksums[right_remote_pos]}"
                                )
                                deduplicated_chunks.append(
                                    DedupReference(
                                        j,
                                        j + (right_remote_pos - left_remote_pos),
                                        left_remote_pos,
                                        right_remote_pos,
                                    )
                                )
                                boarder_j = j + (right_remote_pos - left_remote_pos) + 1

                        current = right_boarder
                        j += 1

                    if deduplicated_chunks:
                        with open(
                            os.path.join(
                                local_tmp_folder, constants.DEDUPLICATION_FILE_NAME
                            ),
                            "w",
                            newline="",
                        ) as csv_file:
                            csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                            for deduplicate_element in deduplicated_chunks:
                                output_dedup = [
                                    deduplicate_element.left_local,
                                    deduplicate_element.right_local,
                                    deduplicate_element.left_remote,
                                    deduplicate_element.right_remote,
                                ]
                                csv_writer.writerow(output_dedup)

                    if not diff_chunks and not deduplicated_chunks:
                        print("File is equal to remote copy, no need to update")
                    else:
                        print(f"file {local_file_name} diffs in {diff_chunks.keys()}")
                        with zipfile.ZipFile(
                            os.path.join(local_tmp_folder, constants.ZIP_ARCHIVE_NAME),
                            "w",
                            zipfile.ZIP_BZIP2,
                        ) as zipf:
                            for diff_chunk_key in diff_chunks.keys():
                                zipf.writestr(
                                    f"{diff_chunk_key}.{local_file[index_of_file_extension + 1 : ]}",
                                    diff_chunks.get(diff_chunk_key),
                                )

                        with open(
                            os.path.join(
                                local_tmp_folder, constants.CHECKSUMS_FILE_NAME
                            ),
                            "w",
                            newline="",
                        ) as csv_file:
                            csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                            csv_writer.writerow(local_checksums)

                        remote_folder_with_version = os.path.join(
                            remote_path,
                            get_relative_root_path(local_path, root),
                            local_file_name,
                            f"v{last_version_number}",
                        )
                        client.sftp_client.mkdir(remote_folder_with_version)

                        for backup_file_in_folder in os.listdir(local_tmp_folder):
                            local_backup_file = os.path.join(
                                local_tmp_folder, backup_file_in_folder
                            )
                            remote_backup_file = os.path.join(
                                remote_folder_with_version, backup_file_in_folder
                            )
                            print(
                                f"localfile = {local_backup_file}; remotefolder = {remote_backup_file}"
                            )
                            client.sftp_client.put(
                                local_backup_file, remote_backup_file
                            )

            else:
                print("new file")
                create_chunked_file(
                    file_path=os.path.join(root, local_file),
                    file_name=local_file,
                    tmp_dir=os.path.join(tmp_dir, get_relative_root_path(local_path, root)),
                )

                remote_folder = os.path.join(
                    remote_path,
                    get_relative_root_path(local_path, root),
                    local_file_name,
                )
                remote_folder_with_version = os.path.join(
                    remote_path,
                    get_relative_root_path(local_path, root),
                    local_file_name,
                    constants.INIT_VERSION_FOLDER_NAME,
                )
                client.sftp_client.mkdir(remote_folder)
                client.sftp_client.mkdir(remote_folder_with_version)
                for backup_file_in_folder in os.listdir(local_tmp_folder):
                    local_backup_file = os.path.join(
                        tmp_dir, get_relative_root_path(local_path, root), local_file_name, backup_file_in_folder
                    )
                    remote_backup_file = os.path.join(
                        remote_folder_with_version, backup_file_in_folder
                    )
                    client.sftp_client.put(local_backup_file, remote_backup_file)

        for subdir in subdirs:
            os.mkdir(
                os.path.join(tmp_dir, get_relative_root_path(local_path, root), subdir)
            )

    shutil.rmtree(tmp_dir)


def recursive_remote_files_init_dir_walk(remote_path, client, remote_hashes_dict):
    remote_files = client.sftp_client.listdir(remote_path)
    for remote_file in remote_files:
        if is_remote_backup_file(remote_path, remote_file, client):
            _, stdout, _ = client.ssh_client.exec_command(
                f"cd {os.path.join(remote_path, remote_file)} && ls -1 | wc -l"
            )
            version = stdout.read().decode("utf-8")
            last_version_number = "v" + version[0 : version.find("\n")]
            checksum_file = os.path.join(
                remote_path,
                remote_file,
                last_version_number,
                constants.CHECKSUMS_FILE_NAME,
            )
            _, stdout, _ = client.ssh_client.exec_command(f"cat {checksum_file}")
            remote_checksums_reader = csv.reader(
                StringIO(stdout.read().decode("utf-8")), delimiter=","
            )
            remote_checksums = [row for row in remote_checksums_reader][0]
            remote_hashes_dict[remote_file] = remote_checksums
        else:
            recursive_remote_files_init_dir_walk(
                os.path.join(remote_path, remote_file), client, remote_hashes_dict
            )


def is_remote_backup_file(remote_path, remote_file_name, client):
    return constants.INIT_VERSION_FOLDER_NAME in client.sftp_client.listdir(
        os.path.join(remote_path, remote_file_name)
    )


def find_remote_checksum_position(remote_checksums, local_checksum_value):
    for i in range(len(remote_checksums)):
        remote_val = remote_checksums[i]
        if remote_val == local_checksum_value:
            return i
    return -1


def find_right_remote_dedup_boarder(
    local_checksums, remote_checksums, local_left_pos, remote_left_pos
):
    local_i, remote_i = local_left_pos, remote_left_pos
    while local_i < len(local_checksums) and remote_i < len(remote_checksums):
        if remote_checksums[remote_i] != local_checksums[local_i]:
            break
        local_i += 1
        remote_i += 1

    return remote_i - 1


def get_relative_root_path(path, root):
    return root[len(path) + 1 :]


class DedupReference:
    def __init__(
        self,
        left_local,
        right_local,
        left_remote,
        right_remote,
    ):
        self.left_local = left_local
        self.right_local = right_local
        self.left_remote = left_remote
        self.right_remote = right_remote
