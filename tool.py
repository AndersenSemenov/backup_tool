from backup_processor import init_backup, incremental_backup_update

init_backup("remote_ip_address", "remote_username", "private_key_file_path",
            "local_path", "remote_path")

incremental_backup_update("remote_ip_address", "remote_username", "private_key_file_path",
                          "local_path", "remote_path")
