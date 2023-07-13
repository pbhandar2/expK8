from setuptools import setup

setup (
    name="expK8",
    version="0.1",
    packages=["expK8.controller", "expK8.remote", "expK8.scheduler", "expK8.experiment"],
    install_requires=["numpy", "pandas", "argparse", "boto3", "psutil", "paramiko"]
)