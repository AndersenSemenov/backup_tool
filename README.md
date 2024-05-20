# JC_Backup
JC_Backup is an efficient backup tool which allows full and incremental backups.

Tool is based on Jump-based chunking (JC) algorithm.

CLI with following commands is provided for the tool usage: *init-full-backup, incremental-backup, recover.* Prompts describing command options are also provided.

In order to install and configure backup tool, you should have python3 preinstalled. After that, several steps should be applied:
1. Create new venv:
```commandline
python3 -m venv .venv
```
2. Activate created venv:
 ```commandline
. .venv/bin/activate
```
3. Install backup tool:
 ```commandline
pip install --editable .
```

Now tool is installed, configured and ready to use. You can access it via **JC_backup** name.