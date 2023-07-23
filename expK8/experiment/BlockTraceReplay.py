import json 

from expK8.controller import RemoteConnect
from expK8.experiment import Experiment


class BlockTraceReplay(Experiment.Experiment):
    """BlockTraceReplay runs block trace replay experiments in remote nodes accumulates and stores the output in 
    a data node. 

    Attributes:
        _remote: RemoteConeect that manages all communication between remote nodes. 
    """
    def __init__(
            self, 
            name, 
            remote_connect
    ) -> None:
        super().__init__(name, remote_connect)
    

    def setup(self) -> None:
        """
        """
        # get all storage devices in the node 

        # mount the devices based on the name of the instance 

        # create a file of size "X" in remote node 

        
        print("setup")