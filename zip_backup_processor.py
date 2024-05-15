import csv
import os
import shutil
import string
import zipfile
from io import StringIO

import paramiko
import xxhash

from chunk_processor import ChunkedFile
from ssh_client import RemoteConnectionClient
from zip_jump_based_chunking import get_right_boarder


def zip_init_backup(
    hostname: string,
    username: string,
    private_key_file_path: string,
    local_path: string,
    remote_path: string,
    tmp_dir: string,
):
    try:
        os.mkdir(tmp_dir)
        chunked_files = []
        recursive_folder(local_path, chunked_files, tmp_dir)

        chunked_files = os.listdir(tmp_dir)
        print(chunked_files)

        client = RemoteConnectionClient(hostname, username, private_key_file_path)
        for root, subdirs, files in os.walk(tmp_dir):
            for subdir in subdirs:
                if is_backup_file_dir(os.path.join(root, subdir)):
                    relative_path = get_relative_root_path(tmp_dir, root)
                    remote_folder = os.path.join(remote_path, relative_path, subdir)
                    remote_folder_with_version = os.path.join(remote_folder, "v1")
                    client.sftp_client.mkdir(remote_folder)
                    client.sftp_client.mkdir(remote_folder_with_version)
                    for backup_file_in_folder in os.listdir(os.path.join(root, subdir)):
                        local_backup_file = os.path.join(root, subdir, backup_file_in_folder)
                        remote_backup_file = os.path.join(remote_folder_with_version, backup_file_in_folder)
                        client.sftp_client.put(local_backup_file, remote_backup_file)
                else:
                    relative_path = get_relative_root_path(tmp_dir, root)
                    remote_folder = os.path.join(remote_path, relative_path, subdir)
                    client.sftp_client.mkdir(remote_folder)
        shutil.rmtree(tmp_dir)
    except paramiko.AuthenticationException as auth_ex:
        print("Authentication failed:", str(auth_ex))
    except paramiko.SSHException as ssh_ex:
        print("SSH connection failed:", str(ssh_ex))
    except paramiko.SFTPError as sftp_ex:
        print("SFTP error:", str(sftp_ex))
    except Exception as e:
        print(str(e))
    finally:
        client.sftp_client.close()
        client.ssh_client.close()


def recursive_folder(path, chunked_files, tmp_dir):
    for root, subdirs, files in os.walk(path):
        for file in files:
            chunked_files.append(
                ChunkedFile(
                    file_path=os.path.join(root, file),
                    file_name=file,
                    tmp_dir=os.path.join(tmp_dir, get_relative_root_path(path, root))
                )
            )
        for subdir in subdirs:
            os.mkdir(os.path.join(tmp_dir, get_relative_root_path(path, root), subdir))


def get_relative_root_path(path, root):
    return root[len(path) + 1 :]


def is_backup_file_dir(current_dir):
    dir_content = os.listdir(current_dir)
    return "archive.zip" in dir_content and "checksums.csv" in dir_content


def get_new_local_checksum_position(remote_checksums, local_checksum_value):
    for i in range(len(remote_checksums)):
        remote_val = remote_checksums[i]
        if remote_val == local_checksum_value:
            return i
    return -1


def get_binary_search_right_dedup_boarder(
    local_checksums, remote_checksums, local_left_pos, remote_left_pos
):
    local_i, remote_i = local_left_pos, remote_left_pos
    while local_i < len(local_checksums) and remote_i < len(remote_checksums):
        if remote_checksums[remote_i] != local_checksums[local_i]:
            break
        local_i += 1
        remote_i += 1

    return remote_i - 1


def zip_incremental_backup_update(
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
    recursive_remote_folder(remote_path, client, remote_hashes_dict)
    print(remote_hashes_dict)

    for root, subdirs, files in os.walk(local_path):
        for local_file in files:
            local_file_name = local_file[0: local_file.find(".")]
            local_tmp_folder = os.path.join(tmp_dir, get_relative_root_path(local_path, root), local_file_name)

            _, stdout, _ = client.ssh_client.exec_command(
                f"cd {os.path.join(remote_path, get_relative_root_path(local_path, root), local_file_name)} && ls -1 | wc -l"
            )
            version = stdout.read().decode("utf-8")
            last_version_number = int(version[0 : version.find("\n")]) + 1

            if local_file_name in remote_hashes_dict:
                remote_checksums = remote_hashes_dict[local_file_name]
                local_checksums = []

                diff_chunks = {}

                with open(os.path.join(root, local_file)) as lf:
                    content = lf.read()
                    content_size = len(content)
                    print(f"GOING TO MAKE DIR - {local_tmp_folder} ON LOCAL FILENAME - {local_file_name}")
                    os.mkdir(local_tmp_folder)

                    j = 0
                    current = 0
                    while current < content_size:
                        right_boarder = get_right_boarder(content, current, content_size)
                        chunk_content = content[current:right_boarder]
                        local_checksums.append(xxhash.xxh32(chunk_content).hexdigest())
                        current = right_boarder
                        j += 1

                    print(f"FILE - {local_file_name}")
                    print(f"local - {local_checksums}")
                    print(f"remote - {remote_checksums}")

                    dedup_structure = []
                    j = 0
                    boarder_j = 0
                    current = 0
                    while (
                            current < content_size
                            and j < len(remote_checksums)
                            and j < len(local_checksums)
                    ):
                        right_boarder = get_right_boarder(content, current, content_size)
                        chunk_content = content[current:right_boarder]

                        if j >= boarder_j and local_checksums[j] != remote_checksums[j]:
                            new_left_local_pos = get_new_local_checksum_position(
                                remote_checksums, local_checksums[j]
                            )
                            right_dedup_boarder = get_binary_search_right_dedup_boarder(
                                local_checksums, remote_checksums, j, new_left_local_pos
                            )

                            if new_left_local_pos == -1:
                                diff_chunks[j] = chunk_content
                                boarder_j += 1
                            else:
                                print(
                                    f"left_val - {remote_checksums[new_left_local_pos]}, "
                                    f"right_val - {remote_checksums[right_dedup_boarder]}"
                                )
                                dedup_structure.append(
                                    DedupReference(
                                        j,
                                        j + (right_dedup_boarder - new_left_local_pos),
                                        new_left_local_pos,
                                        right_dedup_boarder,
                                        version,
                                    )
                                )
                                boarder_j = (
                                        j + (right_dedup_boarder - new_left_local_pos) + 1
                                )

                        current = right_boarder
                        j += 1

                    while current < content_size and j < len(local_checksums):
                        right_boarder = get_right_boarder(content, current, content_size)
                        chunk_content = content[current:right_boarder]

                        if j >= boarder_j:
                            new_left_local_pos = get_new_local_checksum_position(
                                remote_checksums, local_checksums[j]
                            )
                            right_dedup_boarder = get_binary_search_right_dedup_boarder(
                                local_checksums, remote_checksums, j, new_left_local_pos
                            )

                            if new_left_local_pos == -1:
                                diff_chunks[j] = chunk_content
                                boarder_j += 1
                            else:
                                print(
                                    f"left_val - {remote_checksums[new_left_local_pos]}, "
                                    f"right_val - {remote_checksums[right_dedup_boarder]}"
                                )
                                dedup_structure.append(
                                    DedupReference(
                                        j,
                                        j + (right_dedup_boarder - new_left_local_pos),
                                        new_left_local_pos,
                                        right_dedup_boarder,
                                        version,
                                    )
                                )
                                boarder_j = (
                                        j + (right_dedup_boarder - new_left_local_pos) + 1
                                )

                        current = right_boarder
                        j += 1

                    if dedup_structure:
                        with open(
                                os.path.join(local_tmp_folder, "deduplication.csv"), "w", newline=""
                        ) as csv_file:
                            wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                            for dedup in dedup_structure:
                                output_dedup = [
                                    dedup.left_local,
                                    dedup.right_local,
                                    dedup.left_remote,
                                    dedup.right_remote,
                                    dedup.version_name,
                                ]
                                wr.writerow(output_dedup)

                    if not diff_chunks and not dedup_structure:
                        print("File is equal to remote copy, no need to update")
                    else:
                        print(f"file {local_file_name} diffs in {diff_chunks.keys()}")
                        zip_tmp_dir = os.path.join(local_tmp_folder, "archive.zip")
                        with zipfile.ZipFile(zip_tmp_dir, "w", zipfile.ZIP_BZIP2) as zipf:
                            for diff_chunk_key in diff_chunks.keys():
                                zipf.writestr(
                                    f"{diff_chunk_key}.txt", diff_chunks.get(diff_chunk_key)
                                )

                        with open(
                                os.path.join(local_tmp_folder, "checksums.csv"), "w", newline=""
                        ) as csv_file:
                            wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                            wr.writerow(local_checksums)

                        # local_tmp_folder = os.path.join(tmp_dir, local_file_name)
                        remote_folder_with_version = os.path.join(
                            remote_path, get_relative_root_path(local_path, root), local_file_name, f"v{last_version_number}"
                        )
                        client.sftp_client.mkdir(remote_folder_with_version)

                        for backup_file_in_folder in os.listdir(local_tmp_folder):
                            local_backup_file = os.path.join(local_tmp_folder, backup_file_in_folder)
                            remote_backup_file = os.path.join(
                                remote_folder_with_version, backup_file_in_folder
                            )
                            print(f"localfile = {local_backup_file}; remotefolder = {remote_backup_file}")
                            client.sftp_client.put(local_backup_file, remote_backup_file)

            # else:
            #     print("new file")
            #     chunked_file = ChunkedFile(
            #         file_path=os.path.join(local_path, local_file),
            #         file_name=local_file,
            #         tmp_dir=tmp_dir,
            #     )
            #     local_file_name = chunked_file.file_name[
            #                       0: chunked_file.file_name.find(".")
            #                       ]
            #     local_tmp_folder = os.path.join(tmp_dir, local_file_name)
            #     remote_folder = os.path.join(remote_path, local_file_name)
            #     client.sftp_client.mkdir(remote_folder)
            #     remote_folder_with_version = os.path.join(remote_folder, "v1")
            #     client.sftp_client.mkdir(remote_folder_with_version)
            #
            #     for backup_file_in_folder in os.listdir(local_tmp_folder):
            #         lbf = os.path.join(local_tmp_folder, backup_file_in_folder)
            #         rbf = os.path.join(remote_folder_with_version, backup_file_in_folder)
            #         client.sftp_client.put(lbf, rbf)

        for subdir in subdirs:
            os.mkdir(os.path.join(tmp_dir, get_relative_root_path(local_path, root), subdir))

    shutil.rmtree(tmp_dir)


def recursive_remote_folder(remote_path, client, remote_hashes_dict):
    remote_files = client.sftp_client.listdir(remote_path)
    for remote_file in remote_files:
        if is_remote_backup_file(remote_path, remote_file, client):
            _, stdout, _ = client.ssh_client.exec_command(
                f"cd {os.path.join(remote_path, remote_file)} && ls -1 | wc -l"
            )
            version = stdout.read().decode("utf-8")
            last_version_number = "v" + version[0 : version.find("\n")]
            checksum_file = os.path.join(
                remote_path, remote_file, last_version_number, "checksums.csv"
            )
            _, stdout, _ = client.ssh_client.exec_command(f"cat {checksum_file}")
            remote_checksums_reader = csv.reader(StringIO(stdout.read().decode("utf-8")), delimiter=",")
            remote_checksums = [row for row in remote_checksums_reader][0]
            remote_hashes_dict[remote_file] = remote_checksums
        else:
            recursive_remote_folder(os.path.join(remote_path, remote_file), client, remote_hashes_dict)


def is_remote_backup_file(remote_path, remote_file_name, client):
    return "v1" in client.sftp_client.listdir(os.path.join(remote_path, remote_file_name))


class DedupReference:
    def __init__(
        self, left_local, right_local, left_remote, right_remote, version_name
    ):
        self.left_local = left_local
        self.right_local = right_local
        self.left_remote = left_remote
        self.right_remote = right_remote
        self.version_name = version_name
