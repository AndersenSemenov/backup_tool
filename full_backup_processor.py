import csv
import os
import shutil
import string
import zipfile
import constants

import paramiko
import xxhash

from ssh_client import RemoteConnectionClient
from zip_jump_based_chunking import get_right_boarder


def process_full_backup(
    hostname: string,
    username: string,
    private_key_file_path: string,
    local_path: string,
    remote_path: string,
    tmp_dir: string,
):
    try:
        os.mkdir(tmp_dir)
        recursive_chunk_init_dir_walk(local_path, tmp_dir)

        client = RemoteConnectionClient(hostname, username, private_key_file_path)
        for root, subdirs, files in os.walk(tmp_dir):
            for subdir in subdirs:
                if is_backup_file_dir(os.path.join(root, subdir)):
                    relative_path = get_relative_root_path(tmp_dir, root)
                    remote_folder = os.path.join(remote_path, relative_path, subdir)
                    remote_folder_with_version = os.path.join(
                        remote_folder, constants.INIT_VERSION_FOLDER_NAME
                    )
                    client.sftp_client.mkdir(remote_folder)
                    client.sftp_client.mkdir(remote_folder_with_version)
                    for backup_file_in_folder in os.listdir(os.path.join(root, subdir)):
                        local_backup_file = os.path.join(
                            root, subdir, backup_file_in_folder
                        )
                        remote_backup_file = os.path.join(
                            remote_folder_with_version, backup_file_in_folder
                        )
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


def recursive_chunk_init_dir_walk(path, tmp_dir):
    for root, subdirs, files in os.walk(path):
        for file in files:
            create_chunked_file(
                file_path=os.path.join(root, file),
                file_name=file,
                tmp_dir=os.path.join(tmp_dir, get_relative_root_path(path, root)),
            )
        for subdir in subdirs:
            os.mkdir(os.path.join(tmp_dir, get_relative_root_path(path, root), subdir))


def get_relative_root_path(path, root):
    return root[len(path) + 1 :]


def is_backup_file_dir(current_dir):
    dir_content = os.listdir(current_dir)
    return (
            constants.ZIP_ARCHIVE_NAME in dir_content
            and constants.CHECKSUMS_FILE_NAME in dir_content
    )


def create_chunked_file(file_path, file_name, tmp_dir):
    with open(file_path) as file_to_chunk:
        init_chunks_and_checksums_files(file_to_chunk.read(), file_name, tmp_dir)


def init_chunks_and_checksums_files(content: string, file_name, tmp_dir):
    current = 0
    content_size = len(content)
    index_of_file_extension = file_name.find(".")
    local_tmp_folder = os.path.join(
        tmp_dir,
        file_name[0 : index_of_file_extension]
        + "_"
        + file_name[index_of_file_extension + 1 : ]
    )
    os.mkdir(local_tmp_folder)

    checksums = []
    j = 0
    with zipfile.ZipFile(
        os.path.join(local_tmp_folder, constants.ZIP_ARCHIVE_NAME),
            "w",
            zipfile.ZIP_BZIP2
    ) as zipf:
        while current < content_size:
            right_boarder = get_right_boarder(content, current, content_size)
            chunk_content = content[current:right_boarder]
            checksums.append(xxhash.xxh32(chunk_content).hexdigest())
            zipf.writestr(
                f"{j}.{file_name[index_of_file_extension + 1 : ]}", chunk_content
            )
            current = right_boarder
            j += 1
    print(f"number of chunks for file - {file_name} is - {len(checksums)}")

    with open(
        os.path.join(local_tmp_folder, constants.CHECKSUMS_FILE_NAME), "w", newline=""
    ) as csv_file:
        csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        csv_writer.writerow(checksums)
