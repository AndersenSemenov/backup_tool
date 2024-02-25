import os
import string

import paramiko


class RemoteConnectionClient:
    def __init__(self, hostname: string, username: string, private_key_file_path: string):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        private_key = paramiko.Ed25519Key.from_private_key_file(private_key_file_path)
        ssh_client.connect(hostname=hostname, username=username, pkey=private_key)
        self.ssh_client = ssh_client
        self.sftp_client = ssh_client.open_sftp()

    def upload_files(self, local_path, remote_path, file_name):
        self.sftp_client.chdir(remote_path)

        local_file_name = os.path.join(local_path, file_name)
        remote_file_name = os.path.join(remote_path, file_name)
        self.sftp_client.put(local_file_name, remote_file_name, confirm=False)
        print(f"Uploaded {file_name}")



