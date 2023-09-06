"""Config manages the configuration file required to run block trace replay."""

from json import dump
from pathlib import Path 


class Config:
    def __init__(self):
        self.cloudlab_dir = Path("./cloudlab")
        self.nodes = {}
        self.mounts = {
            "c220g1": [
                {
                    "mountpoint": "~/disk",
                    "device": "sdb",
                    "size_gb": 950
                },
                {
                    "mountpoint": "~/nvm",
                    "device": "sdc",
                    "size_gb": 420
                }
            ],
            "c220g5": [
                {
                    "mountpoint": "~/disk",
                    "device": "sdb",
                    "size_gb": 950
                },
                {
                    "mountpoint": "~/nvm",
                    "device": "sda",
                    "size_gb": 420 
                }
            ]
        }
        self.creds = {
            "cloudlab": {
                "user": "pbhandar",
                "type": "file",
                "val": "~/.ssh/id_ed25519"
            },
            "cloudlab_vishwa": {
                "user": "vishwa",
                "type": "file",
                "val": "~/.ssh/id_ed25519"
            },
        }
        self._load()
    

    def _load(self):
        for cloudlab_log_file in self.cloudlab_dir.iterdir():
            with cloudlab_log_file.open("r") as log_handle:
                log_line = log_handle.readline()
                while log_line:
                    if "emulab:vnode" in log_line:
                        host_name = log_line.split("hostname=")[1].split(' ')[0].replace('"', '')
                        name = log_line.split("host name=")[1].split(' ')[0].replace('"', '')
                        machine_type = host_name.split("-")[0]
                        node_name = "{}_{}".format(cloudlab_log_file.stem, name.split(".")[0])
                        cred_name = "cloudlab" if "pbhandar" in log_line else "cloudlab_vishwa"
                        self.nodes[node_name] = {
                            "host": host_name,
                            "cred": cred_name,
                            "mount": machine_type
                        }
                    log_line = log_handle.readline()


    def write_to_file(self, path):
        config = {
            "creds": self.creds,
            "mounts": self.mounts,
            "nodes": self.nodes
        }
        with open(path, "w+") as file_handle:
            dump(config, file_handle, indent=4)


config = Config()
config.write_to_file("config.json")