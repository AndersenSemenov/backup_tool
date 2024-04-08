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
from zip_jump_based_chunking import get_chunk_boarder


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
                print(f"localfile = {lbf}; remotefolder = {rbf}")
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
        remote_hash_list = [row for row in reader][0]
        remote_hashes_dict[remote_folder_name] = remote_hash_list

    for local_file in local_files:
        local_file_name = local_file[0:local_file.find(".")]
        local_tmp_folder = os.path.join(tmp_dir, local_file_name)

        if local_file_name in remote_hashes_dict:
            remote_hash_list = remote_hashes_dict[local_file_name]
            j = 0
            checksums = []
            current = 0

            diff_chunks = {}
            added_chunks = {}
            deleted_chunks = {}

            with open(os.path.join(local_path, local_file)) as lf:
                content = lf.read()
                content_size = len(content)

                while current < content_size:
                    right_boarder = get_chunk_boarder(content, current, content_size)
                    chunk_content = content[current:right_boarder - 1]
                    current_checksum = xxhash.xxh32(chunk_content).hexdigest()
                    checksums.append(current_checksum)

                    if j >= len(remote_hash_list):
                        added_chunks[j] = chunk_content

                    if current_checksum != remote_hash_list[j]:
                        diff_chunks[j] = chunk_content

                    current = right_boarder
                    j += 1

                # main thing in deleted chunking is to save index of deleted chunk
                # no need to save deleted chunk content
                while j < len(remote_hash_list):
                    deleted_chunks[j] = ""
                    j += 1

            if not diff_chunks and not added_chunks and not deleted_chunks:
                print("File is equal to remote copy, no need to update")
            elif diff_chunks or added_chunks or deleted_chunks:
                os.mkdir(local_tmp_folder)
                print(f"file {local_file_name} diffs in {diff_chunks.keys()}")
                zip_tmp_dir = os.path.join(local_tmp_folder, "new.zip")
                with zipfile.ZipFile(zip_tmp_dir, "w", zipfile.ZIP_BZIP2) as zipf:
                    for diff_chunk_key in diff_chunks.keys():
                        zipf.writestr(f"{diff_chunk_key}.txt", diff_chunks.get(diff_chunk_key))
                    for added_chunk_key in added_chunks.keys():
                        zipf.writestr(f"{added_chunk_key}.txt", added_chunks.get(added_chunk_key))

                with open(os.path.join(local_tmp_folder, "checksums.csv"), 'w', newline='') as csv_file:
                    wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                    wr.writerow(checksums)

                local_tmp_folder = os.path.join(tmp_dir, local_file_name)
                # get last version
                # cnt number of folders + 1

                stdin, stdout, stderr = client.ssh_client.exec_command(
                    f"cd {os.path.join(remote_path, local_file_name)} && ls -1 | wc -l")
                version = stdout.read().decode('utf-8')
                last_version_number = int(version[0:version.find("\n")]) + 1
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
                print(f"localfile = {lbf}; remotefolder = {rbf}")
                client.sftp_client.put(lbf, rbf)

    # shutil.rmtree(tmp_dir)
