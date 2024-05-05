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


def zip_init_backup(hostname: string, username: string, private_key_file_path: string,
                    local_path: string, remote_path: string, tmp_dir: string):
    try:
        local_files = os.listdir(local_path)
        os.mkdir(tmp_dir)
        chunked_files = []
        for local_file in local_files:
            chunked_files.append(
                ChunkedFile(file_path=os.path.join(local_path, local_file), file_name=local_file, tmp_dir=tmp_dir))

        chunked_files = os.listdir(tmp_dir)
        print(chunked_files)

        client = RemoteConnectionClient(hostname, username, private_key_file_path)
        for chunked_file in chunked_files:
            local_folder = os.path.join(tmp_dir, chunked_file)
            remote_folder = os.path.join(remote_path, chunked_file)
            client.sftp_client.mkdir(remote_folder)
            remote_folder_with_version = os.path.join(remote_folder, "v1")
            client.sftp_client.mkdir(remote_folder_with_version)

            for backup_file_in_folder in os.listdir(local_folder):
                lbf = os.path.join(local_folder, backup_file_in_folder)
                rbf = os.path.join(remote_folder_with_version, backup_file_in_folder)
                client.sftp_client.put(lbf, rbf)
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


def get_new_local_checksum_position(remote_checksums, local_checksum_value):
    for i in range(len(remote_checksums)):
        remote_val = remote_checksums[i]
        if remote_val == local_checksum_value:
            return i
    return -1


def get_binary_search_right_dedup_boarder(local_checksums, remote_checksums, local_left_pos, remote_left_pos):
    local_i, remote_i = local_left_pos, remote_left_pos
    while local_i < len(local_checksums) and remote_i < len(remote_checksums):
        if remote_checksums[remote_i] != local_checksums[local_i]:
            break
        local_i += 1
        remote_i += 1

    return remote_i - 1


def zip_incremental_backup_update(hostname: string, username: string, private_key_file_path: string,
                                  local_path: string, remote_path: string, tmp_dir: string):
    local_files = os.listdir(local_path)
    os.mkdir(tmp_dir)

    client = RemoteConnectionClient(hostname, username, private_key_file_path)
    remote_hashes_dict = {}

    for remote_folder_name in client.sftp_client.listdir(remote_path):
        stdin, stdout, stderr = client.ssh_client.exec_command(
            f"cd {os.path.join(remote_path, remote_folder_name)} && ls -1 | wc -l")
        version = stdout.read().decode('utf-8')
        last_version_number = "v" + version[0:version.find("\n")]
        checksum_file = os.path.join(remote_path, remote_folder_name, last_version_number, "checksums.csv")
        stdin, stdout, stderr = client.ssh_client.exec_command(f"cat {checksum_file}")
        fff = StringIO(stdout.read().decode('utf-8'))
        reader = csv.reader(fff, delimiter=',')
        remote_checksums = [row for row in reader][0]
        remote_hashes_dict[remote_folder_name] = remote_checksums

    for local_file in local_files:
        local_file_name = local_file[0:local_file.find(".")]
        local_tmp_folder = os.path.join(tmp_dir, local_file_name)

        # get last version
        # cnt number of folders + 1
        stdin, stdout, stderr = client.ssh_client.exec_command(
            f"cd {os.path.join(remote_path, local_file_name)} && ls -1 | wc -l")
        version = stdout.read().decode('utf-8')
        last_version_number = int(version[0:version.find("\n")]) + 1

        if local_file_name in remote_hashes_dict:
            remote_checksums = remote_hashes_dict[local_file_name]
            local_checksums = []

            diff_chunks = {}

            with open(os.path.join(local_path, local_file)) as lf:
                content = lf.read()
                content_size = len(content)
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
                while current < content_size and j < len(remote_checksums) and j < len(local_checksums):
                    right_boarder = get_right_boarder(content, current, content_size)
                    chunk_content = content[current:right_boarder]

                    if j >= boarder_j and local_checksums[j] != remote_checksums[j]:
                        new_left_local_pos = get_new_local_checksum_position(remote_checksums, local_checksums[j])
                        right_dedup_boarder = get_binary_search_right_dedup_boarder(local_checksums, remote_checksums,
                                                                                    j, new_left_local_pos)

                        if new_left_local_pos == -1:
                            diff_chunks[j] = chunk_content
                            boarder_j += 1
                        else:
                            print(
                                f"left_val - {remote_checksums[new_left_local_pos]}, "
                                f"right_val - {remote_checksums[right_dedup_boarder]}")
                            dedup_structure.append(DedupReference(j, j + (right_dedup_boarder - new_left_local_pos),
                                                                  new_left_local_pos, right_dedup_boarder, version))
                            boarder_j = j + (right_dedup_boarder - new_left_local_pos) + 1

                    current = right_boarder
                    j += 1

                while current < content_size and j < len(local_checksums):
                    right_boarder = get_right_boarder(content, current, content_size)
                    chunk_content = content[current:right_boarder]

                    if j >= boarder_j:
                        new_left_local_pos = get_new_local_checksum_position(remote_checksums, local_checksums[j])
                        right_dedup_boarder = get_binary_search_right_dedup_boarder(local_checksums, remote_checksums,
                                                                                    j, new_left_local_pos)

                        if new_left_local_pos == -1:
                            diff_chunks[j] = chunk_content
                            boarder_j += 1
                        else:
                            print(
                                f"left_val - {remote_checksums[new_left_local_pos]}, "
                                f"right_val - {remote_checksums[right_dedup_boarder]}")
                            dedup_structure.append(DedupReference(j, j + (right_dedup_boarder - new_left_local_pos),
                                                                  new_left_local_pos, right_dedup_boarder, version))
                            boarder_j = j + (right_dedup_boarder - new_left_local_pos) + 1

                    current = right_boarder
                    j += 1

            if dedup_structure:
                with open(os.path.join(local_tmp_folder, "deduplication.csv"), 'w', newline='') as csv_file:
                    wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                    for dedup in dedup_structure:
                        output_dedup = [dedup.left_local, dedup.right_local, dedup.left_remote, dedup.right_remote,
                                        dedup.version_name]
                        wr.writerow(output_dedup)

            if not diff_chunks and not dedup_structure:
                print("File is equal to remote copy, no need to update")
            else:
                print(f"file {local_file_name} diffs in {diff_chunks.keys()}")
                zip_tmp_dir = os.path.join(local_tmp_folder, "archive.zip")
                with zipfile.ZipFile(zip_tmp_dir, "w", zipfile.ZIP_BZIP2) as zipf:
                    for diff_chunk_key in diff_chunks.keys():
                        zipf.writestr(f"{diff_chunk_key}.txt", diff_chunks.get(diff_chunk_key))

                with open(os.path.join(local_tmp_folder, "checksums.csv"), 'w', newline='') as csv_file:
                    wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                    wr.writerow(local_checksums)

                local_tmp_folder = os.path.join(tmp_dir, local_file_name)
                remote_folder_with_version = os.path.join(remote_path, local_file_name, f"v{last_version_number}")
                client.sftp_client.mkdir(remote_folder_with_version)

                for backup_file_in_folder in os.listdir(local_tmp_folder):
                    lbf = os.path.join(local_tmp_folder, backup_file_in_folder)
                    rbf = os.path.join(remote_folder_with_version, backup_file_in_folder)
                    print(f"localfile = {lbf}; remotefolder = {rbf}")
                    client.sftp_client.put(lbf, rbf)

        else:
            print("new file")
            chunked_file = ChunkedFile(file_path=os.path.join(local_path, local_file), file_name=local_file,
                                       tmp_dir=tmp_dir)
            local_file_name = chunked_file.file_name[0:chunked_file.file_name.find(".")]
            local_tmp_folder = os.path.join(tmp_dir, local_file_name)
            remote_folder = os.path.join(remote_path, local_file_name)
            client.sftp_client.mkdir(remote_folder)
            remote_folder_with_version = os.path.join(remote_folder, "v1")
            client.sftp_client.mkdir(remote_folder_with_version)

            for backup_file_in_folder in os.listdir(local_tmp_folder):
                lbf = os.path.join(local_tmp_folder, backup_file_in_folder)
                rbf = os.path.join(remote_folder_with_version, backup_file_in_folder)
                client.sftp_client.put(lbf, rbf)

    shutil.rmtree(tmp_dir)


class DedupReference:
    def __init__(self, left_local, right_local,
                 left_remote, right_remote,
                 version_name):
        self.left_local = left_local
        self.right_local = right_local
        self.left_remote = left_remote
        self.right_remote = right_remote
        self.version_name = version_name
