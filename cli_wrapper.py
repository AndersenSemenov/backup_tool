import click

from zip_backup_processor import zip_incremental_backup_update, zip_init_backup


@click.group()
def cli():
    pass


@cli.command()
@click.option("--remote-ip-address")
@click.option("--remote-user")
@click.option("--private-key-file-path")
@click.option("--local-path")
@click.option("--remote-path")
def init_backup(
        remote_ip_address, remote_user, private_key_file_path, local_path, remote_path
):
    click.echo('Init backup')
    zip_init_backup(
        remote_ip_address, remote_user, private_key_file_path, local_path, remote_path
    )


@cli.command()
@click.option("--remote-ip-address")
@click.option("--remote-user")
@click.option("--private-key-file-path")
@click.option("--local-path")
@click.option("--remote-path")
def incremental_backup(
        remote_ip_address, remote_user, private_key_file_path, local_path, remote_path
):
    click.echo("Incremental backup process")
    zip_incremental_backup_update(
        remote_ip_address, remote_user, private_key_file_path, local_path, remote_path
    )


if __name__ == "__main__":
    cli()
