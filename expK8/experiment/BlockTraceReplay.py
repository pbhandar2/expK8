import json 

from expK8.remoteFS import RemoteFS
from expK8.experiment import Experiment


class BlockTraceReplay(Experiment.Experiment):
    """BlockTraceReplay runs block trace replay experiments in remote nodes accumulates and stores the output in 
    a data node. 

    Attributes:
        _remoteFS: RemoteFS that manages all communication between remote nodes. 
    """
    def __init__(
            self, 
            name, 
            machine_id,
            remoteFS
    ) -> None:
        super().__init__(name, remoteFS)
        self.backing_file_path = "~/disk/disk.file"
        self.nvm_file_path = "~/disk/nvm/disk.file"
        self.backing_file_size_gb_dict = {
            "c220g1": 950,
            "c220g5": 950,
            "r6525": 420
        }
        self.nvm_file_size_gb_dict = {
            "c220g1": 420,
            "c220g5": 420,
            "r6525": 1200
        }
        self.backing_file_size_gb = self.backing_file_size_gb_dict[machine_id]
        self.nvm_file_size_gb = self.nvm_file_size_gb_dict[machine_id]


    def setup(self) -> None:
        # create the required files 
        backing_file_dd_cmd = ["dd",
                                "if=/dev/urandom",
                                "of={}".format(self.backing_file_path),
                                "bs=1M",
                                "count=$(({}*1024))".format(self.backing_file_size_gb),
                                "oflag=direct"]
        nvm_file_dd_cmd = ["dd",
                            "if=/dev/urandom",
                            "of={}".format(self.nvm_file_path),
                            "bs=1M",
                            "count=$(({}*1024))".format(self.nvm_file_size_gb),
                            "oflag=direct"]

        # install necessary packages 
        install_cmd = '''
            sudo apt-get -y update 
            sudo apt install -y libaio-dev python3-pip
            cd ~/disk
            git clone https://github.com/pbhandar2/CacheLib
            cd ~/disk/CacheLib 
            git checkout active 
            git clone https://github.com/pbhandar2/phdthesis
            cd ~/disk/CacheLib/phdthesis/cydonia 
            pip3 install . --user
        '''


    def check_if_running(self) -> None:
        # kill all user processes 

        # 
        pass 