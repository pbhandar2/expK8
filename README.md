# expK8
A python package to orchestrate experiments across remote nodes. I am writing this for my PhD thesis where I have to run a large number (1000s) of long running experiments (upto 7 days for a single run) in many ephemeral nodes (1000s of nodes leased and expired). I run expK8 on my personal laptop (control node). The data and experiment output which require high-capacity storage is stored in a cluster in my university (data node). The experiments run on Cloudlab (compute node), a public cloud platform commonly used in academia. I wanted to do the following from my personal laptop (control node) without having to login to any remote resources: 

- schedule experiments to compute nodes 
- cleanup after experiments 
- track live experiments, overall progress, node health
- kill experiments / reset node safely 
- add/remove nodes which automatically adjusts scheduling 
- transfer data to/from data node to compute node 

Note that the control node, data node and compute node can be the same machine as well. It just happens to be different in my case. 


