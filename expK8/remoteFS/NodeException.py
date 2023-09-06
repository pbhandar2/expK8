class BlockDeviceNotFound(Exception):
    """Exception raised when a block device with specified name is not found during node setup.
    
    Args:
        host_name: Host name of the node. 
        block_device_name: The name of the block device that was not found. 
        block_device_name_list: The list of block devices in the node. 
    """
    def __init__(
        self,
        host_name: str, 
        block_device_name: str,
        block_device_name_list: list 
    ) -> None:
        super().__init__("Block devices with name {} not found in remote host {}. \
                The remote host contains the following block devices: {}. \
                ".format(block_device_name, host_name, block_device_name_list))


class NoValidPartitionFound(Exception):
    """Exception raised when no valid partition is found to create a FS on a partition of a specified size.

    Args:
        host_name: Host name of the node. 
        mount_info: Dictionary of mount point information. 
        block_device_info: Dictionary of block devices in the node. 
    """
    def __init__(
        self,
        host_name: str, 
        mount_info: dict, 
        block_device_info: dict
    ) -> None:
        super().__init__("No valid partition found for info: {} in block device {} in host {}.".format(mount_info, block_device_info, host_name))


class RemoteRuntimeError(Exception):
    """Exception raised when the we receive a nonzero exit code from a remote process. """
    def __init__(
            self, 
            remote_cmd: list,
            host_name: str, 
            exit_code: int, 
            stdout: str,
            stderr: str
    ) -> None:
        super().__init__("cmd: {} failed in host{} with exit code {} () \n stdout \n {} \n \
            stderr \n {}".format(" ".join(remote_cmd), host_name, exit_code, stdout, stderr))