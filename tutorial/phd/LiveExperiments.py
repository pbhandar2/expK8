from json import loads 

from expK8.remoteFS.Node import Node 
from NodeFactory import NodeFactory


def is_replay_running(node: Node) -> bool:
    """Check if replay is running in a node.
    
    Args:
        node: Node where experiment might be running. 
    
    Returns:
        running: Boolean indicating if trace replay is already running in the node. 
    """
    running = False 
    ps_output = node.ps()
    for ps_row in ps_output.split('\n'):
        if "bin/cachebench" in ps_row:
            running = True 
            break 
    return running


class LiveExperiments:
    def __init__(self):
        self.node_factory = NodeFactory("../fast24/config.json")
    
    
    def get_live_experiments(self):
        for node in self.node_factory.nodes:
            if not is_replay_running(node):
                continue 

            print("Replay running in node {}".format(node.host))
            config_json_str = node.cat("/run/replay/config.json")
            config_json = loads(config_json_str)

            print(config_json)

            
if __name__ == "__main__":
    live = LiveExperiments()
    live.get_live_experiments()