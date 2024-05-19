from setuptools import setup

setup(
    name="JC_backup",
    version="0.1.0",
    py_modules=["JC_backup"],
    install_requires=[
        "Click",
        "paramiko",
        "xxhash"
    ],
    entry_points={
        "console_scripts": [
            "JC_backup = JC_backup:cli",
        ],
    },
)
