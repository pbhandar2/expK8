from setuptools import setup

setup (
    name="expK8",
    version="0.1",
    packages=["expK8.scheduler", "expK8.experiment", "expK8.remoteFS"],
    install_requires=["numpy", "pandas", "argparse", "boto3", "psutil", "paramiko"]
)