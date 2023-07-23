#! /usr/bin/env bash
#
# Author: Bert Van Vreckem <bert.vanvreckem@gmail.com>
#
#/ Usage: SCRIPTNAME [OPTIONS]... [ARGUMENTS]...
#/
#/ 
#/ OPTIONS
#/   -h, --help
#/                Print this help message
#/
#/ EXAMPLES
#/  


#{{{ Bash settings

# abort on nonzero exitstatus
set -o errexit
# abort on unbound variable
set -o nounset
# don't hide errors within pipes
set -o pipefail

#}}}


#{{{ Variables
#}}}


main() {
  while getopts ":i:" o; do
    case "${o}" in
      i)
        i=${OPTARG}
        ;;
      *)
        usage
        ;;
      esac
  done

  shift $((OPTIND-1))

  if [ -z "${i}" ]; then
    usage
  fi

  if [ "$i" = "c220g1" ]; then
      echo "/dev/sdb"
  else
      echo "Unknown instance type: $i, only " 1>&2; exit 1;
  fi

  BACKING_DIR="~/disk"
  NVM_DIR="~/nvm"

  set_nvm_device $i
  set_backing_store_device $i 
  
  set_nvm_filesize_gb $i 
  set_backing_store_filesize_gb $i 
}


#{{{ Helper functions

usage() { echo "Usage: $0 [-i INSTANCE_TYPE]" 1>&2; exit 1; }


set_backing_store_device() {
  if [ "$1" = "c220g5" ]; then
      backing_store_device="/dev/sdb"
  fi 
}


set_nvm_device() {
  if [ "$1" = "c220g5" ]; then
      nvm_device="/dev/sda4"
  fi 
}


set_backing_store_filesize_gb() {
  if [ "$1" = "c220g1" ]; then
      backing_store_filesize_gb=950 
  fi 
}


set_nvm_filesize_gb() {
  if [ "$1" = "c220g1" ]; then
      backing_store_filesize_gb=420 
  fi 
}


setup_packages() {
  # install libaio and pip3 
  sudo apt-get -y update 
  sudo apt install -y libaio-dev python3-pip

  # install python packages 
  pip3 install psutil boto3 pandas numpy psutil

  # install cmake 
  sudo apt remove --purge --auto-remove -y cmake  

  sudo apt update && \
  sudo apt install -y software-properties-common lsb-release && \
  sudo apt clean all

  wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null

  sudo apt-add-repository "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main"

  sudo apt update
  sudo apt install -y kitware-archive-keyring 
  sudo rm /etc/apt/trusted.gpg.d/kitware.gpg

  sudo apt update
  sudo apt install -y cmake clang 
}


setup_mount() {
  # create the directory where we will mount the backing store and nvm device 
  if [[ ! -d ${BACKING_DIR} ]]; then 
      mkdir ${BACKING_DIR}
  fi 
  if [[ ! -d ${NVM_DIR} ]]; then 
      mkdir ${NVM_DIR}
  fi 

  # check if the backing store and nvm devices are mounted and not the root mount 

  # convert user input of file sizes in GB into bytes 
  backing_file_size_byte=$(( backing_filesize_gb * 1024 * 1024 * 1024))
  nvm_file_size_byte=$(( nvm_filesize_gb * 1024 * 1024 * 1024))

  # use findmnt to check if backing store and nvm device have mountpoints 

  # if those mountpoitns are not equal to our dirs then unmount them 

  # now if they are mounted in the correct dir, then check for file 

  # disk file is already larger or equal then move on 

  
}


#}}}

main "${@}"