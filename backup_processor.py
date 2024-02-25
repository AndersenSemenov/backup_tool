import os
import string

import paramiko

from chunk_processor import ChunkedFile
from ssh_client import RemoteConnectionClient


def init_backup(hostname: string, username: string, private_key_file_path: string,
                local_path: string, remote_path: string):
    try:
        local_files = os.listdir(local_path)
        chunked_files = map(lambda file_name: ChunkedFile(file_path=os.path.join(local_path, file_name), file_name=file_name), local_files)

        client = RemoteConnectionClient(hostname, username, private_key_file_path)
        for chunked_file in chunked_files:
            i = chunked_file.file_name.find(".")
            remote_file_name = chunked_file.file_name[0:i] + "_backup_folder"
            client.sftp_client.chdir(remote_path)
            client.sftp_client.mkdir(remote_file_name)
            client.sftp_client.chdir(remote_file_name)

            j = 0
            for chunk_of_file in chunked_file.file_chunk_list:
                chunk_file_name = chunked_file.file_name[0:i] + f"_{j}" + ".txt"
                remote_chunk_of_file = client.sftp_client.file(chunk_file_name, "a")
                remote_chunk_of_file.write(chunk_of_file)
                remote_chunk_of_file.flush()
                j += 1

                remote_full_path_of_chunk = os.path.join(remote_path, remote_file_name, chunk_file_name)
                client.ssh_client.exec_command(
                    f"setfattr --name=user.checksum --value=hash_value {remote_full_path_of_chunk}")

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
