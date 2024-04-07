import os
import string

import paramiko

from chunk_processor import ChunkedFile
from ssh_client import RemoteConnectionClient
from zip_jump_based_chunking import calculate_checksums


def zip_init_backup(hostname: string, username: string, private_key_file_path: string,
                    local_path: string, remote_path: string, tmp_dir: string):
    try:
        local_files = os.listdir(local_path)
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
    #     todo
    # clean tmp dir
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

    # DONE
    # not zip of whole file, only calculate checksums here
    # and only after comparison
    local_hashes_dict = {}
    for local_file in local_files:
        with open(os.path.join(local_path, local_file)) as lf:
            content = lf.read()
            local_hashes_dict[local_file] = calculate_checksums(content, local_file, tmp_dir)

    print(local_hashes_dict)

    client = RemoteConnectionClient(hostname, username, private_key_file_path)
    remote_hashes_dict = {}

    print("REMOTE")
    os.mkdir(os.path.join(tmp_dir, "local"))
    for remote_folder_name in client.sftp_client.listdir(remote_path):
        checksum_file = os.path.join(remote_path, remote_folder_name, "v1", "checksums.csv")
        # VAR1 CHECKSUM VIA LOCAL FILE
        client.sftp_client.get(remotepath=checksum_file,
                               localpath=os.path.join(tmp_dir, "local", remote_folder_name + "_checksum"))

        # VAR2 CHECKSUM VIA  CAT
        # stdin, stdout, stderr = client.ssh_client.exec_command(f"cat {checksum_file}")
        # print(f"stdout - {stdout.read().decode('utf-8')}")

    # for remote_folder_name in client.sftp_client.listdir():
    #     stdin, stdout, stderr = client.ssh_client.exec_command(
    #         f"cd {os.path.join(remote_path, remote_folder_name)} && getfattr -d -m 'user.checksum' ./*")
    #     matches = re.findall(r"user\.checksum=.*?\n", stdout.read().decode('utf-8'))
    #     remote_hashes_dict[remote_folder_name] = (list(map(lambda el: el[el.find('=') + 2:-2], matches)))
    #
    # print(f"remote_hashes_dict - {remote_hashes_dict}")
    #
    # for chunked_file in chunked_files:
    #     ind = chunked_file.file_name.find(".")
    #     remote_file_name = chunked_file.file_name[0:ind] + "_backup_folder"
    #     remote_hashes = remote_hashes_dict[remote_file_name]
    #
    #     local_hashes = chunked_file.checksum_list
    #     diff_chunks = []
    #     added_chunks = []
    #     deleted_chunks = []
    #
    #     print(f"remote hashes for file {remote_file_name} is {remote_hashes}")
    #     print(f"local hashes for file {remote_file_name} is {local_hashes}")
    #
    #     for i in range(min(len(local_hashes), len(remote_hashes))):
    #         if local_hashes[i] != remote_hashes[i]:
    #             diff_chunks.append(i)
    #
    #     #     new chunks added or deleted, need to upload them to remote
    #     #     added_chunks/deleted_chunks lists
    #     if len(local_hashes) > len(remote_hashes):
    #         for i in range(len(remote_hashes), len(local_hashes)):
    #             added_chunks.append(i)
    #     elif len(remote_hashes) > len(local_hashes):
    #         for i in range(len(local_hashes), len(remote_hashes)):
    #             deleted_chunks.append(i)
    #
    #     if not diff_chunks or not added_chunks or not deleted_chunks:
    #         print(f"File in differs in chunks with nums - {diff_chunks}, "
    #               f"added chunks with nums - {added_chunks}, deleted chunks with nums - {deleted_chunks}")
    #
    #         remote_file_name = chunked_file.file_name[0:ind] + "_backup_folder"
    #         client.sftp_client.chdir(remote_file_name)
    #
    #         for diff_chunk in diff_chunks:
    #             chunk_file_name = chunked_file.file_name[0:ind] + f"_{diff_chunk}" + ".txt"
    #             remote_chunk_of_file = client.sftp_client.file(chunk_file_name, "w")
    #             new_content = chunked_file.file_chunk_list[diff_chunk]
    #             remote_chunk_of_file.write(new_content)
    #             remote_chunk_of_file.flush()
    #
    #             remote_full_path_of_chunk = os.path.join(remote_path, remote_file_name, chunk_file_name)
    #             client.ssh_client.exec_command(
    #                 f"setfattr --name=user.checksum --value={chunked_file.checksum_list[diff_chunk]} {remote_full_path_of_chunk}")
    #         for added_chunk in added_chunks:
    #             chunk_file_name = chunked_file.file_name[0:ind] + f"_{added_chunk}" + ".txt"
    #             remote_chunk_of_file = client.sftp_client.file(chunk_file_name, "a")
    #             new_content = chunked_file.file_chunk_list[added_chunk]
    #             remote_chunk_of_file.write(new_content)
    #             remote_chunk_of_file.flush()
    #
    #             remote_full_path_of_chunk = os.path.join(remote_path, remote_file_name, chunk_file_name)
    #             client.ssh_client.exec_command(
    #                 f"setfattr --name=user.checksum --value={chunked_file.checksum_list[added_chunk]} {remote_full_path_of_chunk}")
    #         for deleted_chunk in deleted_chunks:
    #             chunk_file_name = chunked_file.file_name[0:ind] + f"_{deleted_chunk}" + ".txt"
    #             client.sftp_client.remove(chunk_file_name)
    #     else:
    #         print("File is equal to remote copy")


def get_remote_file_name_by_local():
    return


def get_local_file_name_by_remote():
    return
