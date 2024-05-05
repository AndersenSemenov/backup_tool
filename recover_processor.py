import os
import stat
import string
import zipfile

from ssh_client import RemoteConnectionClient


def recover_file(hostname: string, username: string, private_key_file_path: string,
                 local_path: string, remote_path: string, tmp_dir: string):
    client = RemoteConnectionClient(hostname, username, private_key_file_path)
    os.mkdir(tmp_dir)
    os.mkdir(os.path.join(tmp_dir, 'backup_data'))
    os.mkdir(os.path.join(tmp_dir, 'recover_data'))
    os.mkdir(os.path.join(tmp_dir, 'buffer'))
    recursive_sftp_get(client.sftp_client, remote_path, os.path.join(tmp_dir, 'backup_data'))

    for tmp_local_file_folder in os.listdir(os.path.join(tmp_dir, 'backup_data')):
        os.mkdir(os.path.join(tmp_dir, 'buffer', tmp_local_file_folder))
        version_number = len(next(os.walk(os.path.join(tmp_dir, 'backup_data', tmp_local_file_folder)))[1])
        print(f'For folder - {tmp_local_file_folder} version is - {version_number}')

        chunks_positions = {}
        while version_number > 0:
            # unzip
            with zipfile.ZipFile(
                    os.path.join(tmp_dir, 'backup_data', tmp_local_file_folder, 'v' + str(version_number), 'new.zip'),
                    'r') as zip_ref:
                zip_ref.extractall(
                    os.path.join(os.path.join(tmp_dir, 'buffer', tmp_local_file_folder, 'v' + str(version_number))))

            # iterate through extracted files and get chunk numbers
            for backup_chunk in os.listdir(
                    os.path.join(os.path.join(tmp_dir, 'buffer', tmp_local_file_folder, 'v' + str(version_number)))):
                backup_chunk_name = backup_chunk[0:backup_chunk.find('.')]
                if backup_chunk_name not in chunks_positions:
                    chunks_positions[backup_chunk_name] = version_number

            version_number -= 1

        # based on chunks positions merge chunks to get file
        with open(os.path.join(tmp_dir, 'recover_data', tmp_local_file_folder + '.txt'), 'a') as rf:
            for chunk_number in range(len(chunks_positions)):
                chunk_location_folder = 'v' + str(chunks_positions[str(chunk_number)])
                chunk_path = os.path.join(tmp_dir, 'buffer', tmp_local_file_folder, chunk_location_folder,
                                          str(chunk_number) + '.txt')
                rf.write(open(chunk_path).read())

        # os.rmdir(os.path.join(tmp_dir, 'buffer', tmp_local_file_folder))


def recursive_sftp_get(sftp, remote_path, local_path):
    for item in sftp.listdir_attr(remote_path):
        remote_item = os.path.join(remote_path, item.filename)
        local_item = os.path.join(local_path, item.filename)

        if stat.S_ISDIR(item.st_mode):
            os.makedirs(local_item, exist_ok=True)
            recursive_sftp_get(sftp, remote_item, local_item)
        else:
            sftp.get(remote_item, local_item)
