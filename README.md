# Pandora: Fast, Recoverable, Highly Available Transactions on Disaggregated Datastores

Pandor is first one-sided transactional protocol that is specifically designed to enable fast and correct recovery on disaggregated KVSes. Pandora's fast recovery hinges on two innovations: (a) the PILL (Pandora's Implicit Latch Logging), a novel technique for managing latches in the presence of compute failures; and (b) an RDMA-based recovery algorithm that detects and quickly recovers from failures. 
To validate that Pandora recovers correctly in the presence of failures, we introduce a new litmus-testing framework for end-to-end validation of transactional protocols. Our evaluation (and validation) reveals that Pandora achieves fast and correct recovery in the range of a few milliseconds without compromising the performance of failure-free runtime execution.


# Prerequisites to Build

- Hardware
  - Mellanox InfiniBand NIC (e.g., ConnectX-5) that supports RDMA
  - Mellanox InfiniBand Switch
- Software
  - Operating System: Ubuntu 18.04 LTS or above
  - Mellanox OFED 2.4+
  - Programming Language: C++ 11 or above 
  - Compiler: g++ 7.5.0 (at least)
  - Boost: 1.60.1 (this must be selected based on the g++ version)
  - gRPC:  v1.61.x
  - Zookeeper: zookeeper-3.7.1 or above (with Maven 3.8.8 +)
  - Libraries: ibverbs, pthread, boost_coroutine, boost_context, boost_system, libnuma-dev
- Machines
  - 4 machines, two acts as the compute pool and other two act as the memory pool to maintain a primary-backup replication
  - 3 servers for failures detector (You can use the same servers used for compute and memory, but not recommend)
 
# Setting Up
We have two ways to configure, build and run experiments. Normally, we run our experiments on 8 cloudlab servers with a preconfigured ubuntu image. Additionally, you can build the this project manually and run experiments. In this section, we briefly explain both apporaches. 

## Cloudlab
We use 8 r650 cloudlab servers for the experiments becuase sometimes some servers are faulty in which case we need to manuallty set up. 

- RDMA cluster with 8 servers, IPs starting from 10.1.1.1 to 10.1.1.8
- Install the required software on each server.
- Check the connectivity of cloudlab
- Get our cloudlab configuration files

```sh
$ git clone https://github.com/maheshdananjaya/cloudlab-config.git
$ cd cloudlab-configure
```

- Change the IP addressed of servers in cx5_init_cloudlab.sh, and then run

```sh
$./cx5_init_cloudlab.sh
```
NOTE: You can also run all the experiments in one go (Refer to 'Running Experiments' section). However, we highly recommend to check the connectivity and the cluster in case if something is faulty or misconfigured. 

## Manually Setting Up
You need to first install all the required software on each machine. You can then use these scripts in your setup. But make sure you have ssh permission to access all 8 servers. 

- Get cloudlab-config from github (You can manually do the steos in cx5_init_cloulab.sh steps)
```sh
git clone https://github.com/maheshdananjaya/cloudlab-config.git
```
- Copy cx5_init-preimage.sh, startexp.sh, termexp.sh, startfd.sh, termfd.sh, and setBENCH.sh scripts to all servers
- run cx5_init-preimaged.sh in all servers
```sh
./cx5_init-preimaged.sh
```
- Change IPs in 1KVS/config ffiles accordingly. Particulaly, compute_node_config.json and memory_node_config.json.

# Running Experiments
 We typically start out experients from node-2 which is 10.1.1.2. You can do the following with the correct ip.

- You can run experients from your machine or from 10.1.1.2. For exmaple you can fail-over throughut experiments like

```sh
ssh 10.1.1.2
cd 1KVS/scripts
./run_failover_tput.sh
```


# Extracting Results
All the results are saved into results/ folder. We usually get these files and load them into google docs which we use to generate plots.
