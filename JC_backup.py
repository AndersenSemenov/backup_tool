import click

from full_backup_processor import process_full_backup
from incremental_backup_processor import process_incremental_backup
from recover_backup_processor import recover_file


@click.group()
def cli():
    pass


@cli.command()
@click.option("--remote-ip", prompt="IP address of remote backup server machine")
@click.option("--remote-user", prompt="Username of remote backup server machine")
@click.option("--ssh-key-file-path", prompt="Local path to ssh key bounded with remote server")
@click.option("--local-path", prompt="Backup local path")
@click.option("--remote-path", prompt="Backup remote path")
@click.option("--tmp-path", prompt="Path for temporary files created while backuping")
def init_full_backup(
    remote_ip,
    remote_user,
    ssh_key_file_path,
    local_path,
    remote_path,
    tmp_path,
):
    click.echo("Init first full backup")
    process_full_backup(
        remote_ip,
        remote_user,
        ssh_key_file_path,
        local_path,
        remote_path,
        tmp_path,
    )


@cli.command()
@click.option("--remote-ip", prompt="IP address of remote backup server machine")
@click.option("--remote-user", prompt="Username of remote backup server machine")
@click.option("--ssh-key-file-path", prompt="Local path to ssh key bounded with remote server")
@click.option("--local-path", prompt="Backup local path")
@click.option("--remote-path", prompt="Backup remote path")
@click.option("--tmp-path", prompt="Path for temporary files created while backuping")
def incremental_backup(
    remote_ip,
    remote_user,
    ssh_key_file_path,
    local_path,
    remote_path,
    tmp_path,
):
    click.echo("Incremental backup process")
    process_incremental_backup(
        remote_ip,
        remote_user,
        ssh_key_file_path,
        local_path,
        remote_path,
        tmp_path,
    )


@cli.command()
@click.option("--remote-ip", prompt="IP address of remote backup server machine")
@click.option("--remote-user", prompt="Username of remote backup server machine")
@click.option("--ssh-key-file-path", prompt="Local path to ssh key bounded with remote server")
@click.option("--remote-path", prompt="Backup remote path")
@click.option("--tmp-path", prompt="Path for temporary files created while backuping")
def recover(
    remote_ip,
    remote_user,
    ssh_key_file_path,
    remote_path,
    tmp_path,
):
    click.echo("Incremental backup process")
    recover_file(
        remote_ip,
        remote_user,
        ssh_key_file_path,
        remote_path,
        tmp_path,
    )


if __name__ == "__main__":
    cli()
