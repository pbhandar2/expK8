"""This script kills all live experiments in remote nodes."""

from NodeFactory import NodeFactory
from Clean import kill_trace_replay, is_replay_running


if __name__ == "__main__":
    lock = True 
    if lock:
        print("This is the lock set for protection. Now unset this lock and run this script again if you are sure you want to kill replay in all nodes.")
    else:    
        node_factory = NodeFactory("../fast24/config.json")
        for node in node_factory.get_node_list():
            kill_trace_replay(node)
        
        print("Killed all trace replay.")
        for node in node_factory.get_node_list():
            print("Node: {}, replay: {}".format(node.host, is_replay_running(node)))
