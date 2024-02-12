import os
import string

import paramiko

from chunk_processor import ChunkedFile
from ssh_client import RemoteConnectionClient


def init_backup(hostname: string, username: string, private_key_file_path: string,
                local_path: string, remote_path: string):
    try:
        local_files = os.listdir(local_path)
        chunked_file = map(lambda file_name: ChunkedFile(file_path=os.path.join(local_path, file_name)), local_files)

        for chunk in chunked_file:
            client = RemoteConnectionClient(hostname, username, private_key_file_path)
            client.upload_files(local_path, remote_path, chunk.chunk_list)
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
