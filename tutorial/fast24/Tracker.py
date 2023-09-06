from json import load 
from time import sleep 

from expK8.remoteFS.RemoteFS import RemoteFS

from RunExperiment import is_replay_running


class Tracker:
    def __init__(
        self, 
        config_file_path="config.json"
    ) -> None:
        self.config_file_path = config_file_path
    

    def start_tracking(self):
        while True:
            with open(self.config_file_path, "r") as config_file_handle:
                fs_config = load(config_file_handle)
            fs = RemoteFS(fs_config)

            for host_name in fs.get_all_live_host_names():
                node = fs.get_node(host_name)
                print(host_name, is_replay_running(node))

                if is_replay_running(node):
                    print("{}: {}: Live".format(host_name, node.name))
                else:
                    if node.get_file_size("/dev/shm/tracereplay/stat0.out"):
                        print("{}: Complete experiment detected".format(host_name))
                    else:
                        print("{}: Not running".format(host_name))
            break 


if __name__ == "__main__":
    tracker = Tracker()
    tracker.start_tracking() 