import csv
import os
import stat
import string
import zipfile
import constants

from incremental_backup_processor import DedupReference
from ssh_client import RemoteConnectionClient


def recover_file(
    hostname: string,
    username: string,
    private_key_file_path: string,
    remote_path: string,
    tmp_dir: string,
):
    client = RemoteConnectionClient(hostname, username, private_key_file_path)
    os.mkdir(tmp_dir)
    os.mkdir(os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME))
    os.mkdir(os.path.join(tmp_dir, constants.RECOVERED_DATA_FOLDER_NAME))
    os.mkdir(os.path.join(tmp_dir, constants.BUFFER_FOLDER_NAME))
    recursive_get_remote_backup_data(
        client.sftp_client,
        remote_path,
        os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME),
    )

    for root, subdirs, files in os.walk(
        os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME)
    ):
        for subdir in subdirs:
            if is_backup_file_folder(os.path.join(root, subdir)):
                last_version_number = len(next(os.walk(os.path.join(root, subdir)))[1])
                backup_file_extension_name = subdir[subdir.find("_") + 1 :]

                chunks_positions = {}
                version_number = last_version_number
                while version_number > 0:
                    # unzip
                    with zipfile.ZipFile(
                        os.path.join(
                            root,
                            subdir,
                            "v" + str(version_number),
                            constants.ZIP_ARCHIVE_NAME,
                        ),
                        "r",
                    ) as zip_ref:
                        zip_ref.extractall(
                            os.path.join(
                                tmp_dir,
                                constants.BUFFER_FOLDER_NAME,
                                get_relative_root_path(
                                    os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME),
                                    root,
                                ),
                                subdir,
                                "v" + str(version_number),
                            )
                        )

                    # iterate through extracted files and get chunk numbers
                    for backup_chunk in os.listdir(
                        os.path.join(
                            tmp_dir,
                            constants.BUFFER_FOLDER_NAME,
                            get_relative_root_path(
                                os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME),
                                root,
                            ),
                            subdir,
                            "v" + str(version_number),
                        )
                    ):
                        backup_chunk_name = backup_chunk[0 : backup_chunk.find(".")]
                        if backup_chunk_name not in chunks_positions:
                            chunks_positions[backup_chunk_name] = version_number

                    version_number -= 1

                # deduplication process
                version_number = last_version_number
                dedup_all = {}
                while version_number > 0:
                    # check if deduplication.csv exists in current version
                    path_to_current_dedup = os.path.join(
                        root,
                        subdir,
                        "v" + str(version_number),
                        constants.DEDUPLICATION_FILE_NAME,
                    )
                    if os.path.isfile(path_to_current_dedup):
                        # parse deduplication.csv
                        deduplicated_chunks = []
                        with open(
                            os.path.join(
                                root,
                                subdir,
                                "v" + str(version_number),
                                constants.DEDUPLICATION_FILE_NAME,
                            )
                        ) as csv_file:
                            csv_reader = csv.reader(csv_file, delimiter=",")
                            for row in csv_reader:
                                deduplicated_chunks.append(
                                    DedupReference(
                                        int(row[0]),
                                        int(row[1]),
                                        int(row[2]),
                                        int(row[3]),
                                    )
                                )
                        dedup_all[version_number] = deduplicated_chunks

                    version_number -= 1

                version_number = last_version_number
                dedup_chunks_positions = {}
                while version_number > 0:
                    if version_number in dedup_all:
                        dedup_structure = dedup_all[version_number]
                        for dedup_element in dedup_structure:
                            for dedup_chunk_number in range(
                                dedup_element.left_local, dedup_element.right_local + 1
                            ):
                                if dedup_chunk_number not in chunks_positions:
                                    dedup_chunks_positions[dedup_chunk_number] = (
                                        find_dedup_version(
                                            version_number - 1,
                                            dedup_element.left_remote
                                            + (
                                                dedup_chunk_number
                                                - dedup_element.left_local
                                            ),
                                            dedup_all,
                                        )
                                    )
                    version_number -= 1

                print(f"dedup_chunks_positions - {dedup_chunks_positions}")

                with open(
                    os.path.join(
                        tmp_dir,
                        constants.RECOVERED_DATA_FOLDER_NAME,
                        get_relative_root_path(
                            os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME), root
                        ),
                        subdir[0 : subdir.find("_")] + "." + backup_file_extension_name,
                    ),
                    "a",
                ) as rf:
                    range_val = get_final_total_number_of_chunks(
                        os.path.join(
                            root,
                            subdir,
                            "v" + str(last_version_number),
                            constants.CHECKSUMS_FILE_NAME,
                        )
                    )
                    for chunk_number in range(range_val):
                        if chunk_number not in dedup_chunks_positions:
                            chunk_location_folder = "v" + str(
                                chunks_positions[str(chunk_number)]
                            )
                            chunk_path = os.path.join(
                                tmp_dir,
                                constants.BUFFER_FOLDER_NAME,
                                get_relative_root_path(
                                    os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME),
                                    root,
                                ),
                                subdir,
                                chunk_location_folder,
                                str(chunk_number) + "." + backup_file_extension_name,
                            )
                        else:
                            chunk_location_folder = "v" + str(
                                dedup_chunks_positions[chunk_number].version_number
                            )
                            chunk_path = os.path.join(
                                tmp_dir,
                                constants.BUFFER_FOLDER_NAME,
                                get_relative_root_path(
                                    os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME),
                                    root,
                                ),
                                subdir,
                                chunk_location_folder,
                                str(dedup_chunks_positions[chunk_number].chunk_number)
                                + "."
                                + backup_file_extension_name,
                            )
                        rf.write(open(chunk_path).read())

                # os.rmdir(os.path.join(tmp_dir, 'buffer', tmp_local_file_folder))
            elif not os.path.exists(
                os.path.join(
                    tmp_dir,
                    constants.BUFFER_FOLDER_NAME,
                    get_relative_root_path(
                        os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME), root
                    ),
                    subdir,
                )
            ):
                os.mkdir(
                    os.path.join(
                        tmp_dir,
                        constants.BUFFER_FOLDER_NAME,
                        get_relative_root_path(
                            os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME), root
                        ),
                        subdir,
                    )
                )
                os.mkdir(
                    os.path.join(
                        tmp_dir,
                        constants.RECOVERED_DATA_FOLDER_NAME,
                        get_relative_root_path(
                            os.path.join(tmp_dir, constants.BACKUP_FOLDER_NAME), root
                        ),
                        subdir,
                    )
                )


def get_final_total_number_of_chunks(path):
    with open(path) as checksums_file:
        checksums = csv.reader(checksums_file)
        total_number_of_chunks = 0
        for row in checksums:
            total_number_of_chunks += len(row)
        return total_number_of_chunks


def get_dedup_max(dedup_chunks_positions):
    current_max = -1
    for key in dedup_chunks_positions.keys():
        if key > current_max:
            current_max = key
    return current_max


def find_dedup_version(version_number, dedup_chunk_number, dedup_all):
    while True:
        if version_number in dedup_all:
            current_dedup_structure = dedup_all[version_number]
            is_found = False
            for dedup_element in current_dedup_structure:
                # apply step of search again
                if (
                    dedup_element.left_local >= dedup_chunk_number
                    and dedup_chunk_number <= dedup_element.right_local
                ):
                    is_found = True
                    dedup_chunk_number = dedup_element.left_remote + (
                        dedup_chunk_number - dedup_element.left_local
                    )
                    break

            if not is_found:
                return RecoverPair(version_number, dedup_chunk_number)
        else:
            return RecoverPair(version_number, dedup_chunk_number)

        version_number -= 1


def recursive_get_remote_backup_data(sftp, remote_path, local_path):
    for item in sftp.listdir_attr(remote_path):
        remote_item = os.path.join(remote_path, item.filename)
        local_item = os.path.join(local_path, item.filename)

        if stat.S_ISDIR(item.st_mode):
            os.makedirs(local_item, exist_ok=True)
            recursive_get_remote_backup_data(sftp, remote_item, local_item)
        else:
            sftp.get(remote_item, local_item)


def is_backup_file_folder(path):
    return constants.INIT_VERSION_FOLDER_NAME in os.listdir(path)


def get_relative_root_path(path, root):
    return root[len(path) + 1 :]


class RecoverPair:
    def __init__(self, version_number, chunk_number):
        self.version_number = version_number
        self.chunk_number = chunk_number
