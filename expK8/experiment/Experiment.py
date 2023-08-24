from abc import ABC 
from expK8.remoteFS import RemoteFS
from pathlib import Path 


class Experiment(ABC):
    """An abstract class that defines experiments that can be run in expK8. 

    Attributes:
        name: Name used to generate output and track status. 
        remoteFS: RemoteFS to communicate to remote nodes and make them communicate to each other. 
        _logger: Logger that updates the log file related to this experiment. 
    """
    def __init__(
            self,
            name, 
            remoteFS: RemoteFS
    ) -> None:
        self.name = name 
        self.remoteFS = remoteFS
    

    def setup(self):
        raise NotImplementedError("Setup function not implemented for this experiment.")
    

    def run(self):
        raise NotImplementedError("Run function not implemented for this experiment.")


    def clean(self):
        raise NotImplementedError("Clean function not implemented for this experiment.")