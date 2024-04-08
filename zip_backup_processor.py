import csv
import os
import shutil
import string
import zipfile
from io import StringIO

import paramiko

from chunk_processor import ChunkedFile
from ssh_client import RemoteConnectionClient
from zip_jump_based_chunking import calculate_checksums


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

    # DONE
    # not zip of whole file, only calculate checksums here
    # and only after comparison
    local_hashes_dict = {}
    for local_file in local_files:
        with open(os.path.join(local_path, local_file)) as lf:
            content = lf.read()
            local_file_name = local_file[0:local_file.find(".")]
            local_hashes_dict[local_file_name] = calculate_checksums(content, local_file, tmp_dir)

    client = RemoteConnectionClient(hostname, username, private_key_file_path)
    remote_hashes_dict = {}

    # os.mkdir(os.path.join(tmp_dir, "local"))
    for remote_folder_name in client.sftp_client.listdir(remote_path):
        checksum_file = os.path.join(remote_path, remote_folder_name, "v1", "checksums.csv")
        # VAR2 CHECKSUM VIA  CAT
        stdin, stdout, stderr = client.ssh_client.exec_command(f"cat {checksum_file}")
        remote_hashes_dict[remote_folder_name] = stdout.read().decode('utf-8')

    # print(f"local_hashes_dict - {local_hashes_dict}")
    # print(f"remote_hashes_dict - {remote_hashes_dict}")

    for pair in local_hashes_dict:
        local_file_name
        try:
            local_file_name = pair
        except ValueError:
            print("error")
        diff_chunks = []
        added_chunks = []
        deleted_chunks = []

        local_hash_list = local_hashes_dict.get(local_file_name)

        if local_file_name in remote_hashes_dict:
            fff = StringIO(remote_hashes_dict[local_file_name])
            reader = csv.reader(fff, delimiter=',')
            remote_hash_list = [row for row in reader][0]

            print(f"FOR FILE {local_file_name}")
            # print(f"remote hashes are {remote_hash_list}")
            # print(f"local hashes are {local_hash_list}")

            for i in range(min(len(local_hash_list), len(remote_hash_list))):
                if local_hash_list[i] != remote_hash_list[i]:
                    diff_chunks.append(i)

            #     new chunks added or deleted, need to upload them to remote
            #     added_chunks/deleted_chunks lists
            if len(local_hash_list) > len(remote_hash_list):
                for i in range(len(remote_hash_list), len(local_hash_list)):
                    added_chunks.append(i)
            elif len(remote_hash_list) > len(local_hash_list):
                for i in range(len(local_hash_list), len(remote_hash_list)):
                    deleted_chunks.append(i)

            print(f"diff_chunks - {diff_chunks}")
            print(f"added_chunks - {added_chunks}")
            print(f"deleted_chunks - {deleted_chunks}")
            print()

            if diff_chunks or added_chunks or deleted_chunks:
                print(f"File in differs in chunks with nums - {diff_chunks}, "
                      f"added chunks with nums - {added_chunks}, deleted chunks with nums - {deleted_chunks}")

                remote_folder_with_version = os.path.join(remote_path, local_file_name, "v2")
                client.sftp_client.mkdir(remote_folder_with_version)

                zip_tmp_dir = os.path.join(tmp_dir)
                with zipfile.ZipFile(zip_tmp_dir, "w", zipfile.ZIP_BZIP2) as zipf:
                    for diff_chunk in diff_chunks:
                        zipf.writestr(f"{diff_chunk}.txt", "chunk_content")

                    for added_chunk in added_chunks:
                        zipf.writestr(f"{added_chunk}.txt", "chunk_content")

                # TODO
                # process deleted
                # for deleted_chunk in deleted_chunks:
                #     chunk_file_name = f"{deleted_chunk}" + ".txt"
            else:
                print("File is equal to remote copy")

    shutil.rmtree(tmp_dir)
