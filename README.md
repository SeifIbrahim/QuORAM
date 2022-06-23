
QuORAM: A Quorum-Replicated Fault Tolerant ORAM Datastore
=========================================================
QuORAM is the first ORAM datastore to replicate data with a quorum-based replication protocol.
QuORAM’s contributions are three-fold: (i) it obfuscates access patterns to provide obliviousness guarantees,
(ii) it replicates data using a novel lock-free and decentralized replication protocol to achieve fault-tolerance, and (iii) it guarantees linearizable semantics.

This implementation accompanies our paper [QuORAM: A Quorum-Replicated Fault Tolerant ORAM Datastore](https://eprint.iacr.org/2022/691.pdf) by Sujaya Maiyya, Seif Ibrahim, Caitlin Scarberry, Divyakant Agrawal, Amr El Abbadi, Huijia Lin, Stefano Tessaro and Victor Zakhary.

This repo contains the implementation of QuORAM as well as the insecure replication baseline refered to in the code as the "Lynch" baseline. We use another [repo for our CockroachDB baseline and Unreplicated Secure baseline](https://github.com/SeifIbrahim/TaoStore).

We run our experiments using our [AWS scripts](https://github.com/SeifIbrahim/quoram-experiments). This is the main repo you should refer to in order to run our QuORAM experiments from the paper.

Local Setup
-----------
All dependencies are included in the lib directory, you will need to have Java 11 installed.

To compile, navigate to the repository directory and run `./scripts/compile-commands.sh`.

To run a load test on your local machine:

 * TaoServer: `./scripts/run-server.sh <unit_id>`
 * TaoProxy: `./scripts/run-proxy.sh <unit_id>`
 * TaoClient: `./scripts/run-client.sh <num_clients> <loadtest_length> <rw_ratio> <zipf> <num_warmup>  <quorum_type> <client_id>`

If you have tmux installed you can simply execute `./scripts/run-tmux.sh` which will run a load test with 3 replicas. You will be able to attach to the tmux sessions to see outputs from the clients, proxies, and servers.

***** 
  
**Command Line Arguments:**
 * For TaoClient, TaoProxy, TaoServer
 * --config_file full_path_to_file
    * The full path to a config file 
    * Default file is *config.properties*
    * Any properties not specified in config file will take default values from `src/main/resources/Configuration/TaoDefaultConfigs`
 * For TaoClient and TaoProxy
  * --unit
    * The unit number of the TaoClient and TaoProxy.
    * Must be specified.
 * For TaoClient
 * --runType
    * Can be either *interactive* or *load_test*
    * Default is *interactive*
    * An interactive lets the user do manual operations 
    * A load test runs a load test
    * Note that a load test begins after the client does *warmup_operations* operations
 * --clients
    * Specifies the number of concurrent clients that should run during the load test
    * Default is 1
 * --warmup_operations
    * The amount of operations to perform before beginning the load test
    * Default is 100
 * --load_test_length
    * Length in milliseconds of load test
    * Default is 2 minutes
 * --id
    * Client ID
    * Only used in interactive mode.
    * If you run multiple interactive clients at once, they must have different IDs.

***** 

**Example Usage**
In this example, we'll start three replica units and one TaoClient.

First, check `config.properties`. By default, the configuration is set up to run three replica units and a client on one machine. Verify that all port numbers listed in the config file are free on your machine. 

In separate terminals, from the `target` directory:

 * `./scripts/run-server.sh 0`
 * `./scripts/run-proxy.sh 0`
 * `./scripts/run-server.sh 1`
 * `./scripts/run-proxy.sh 1`
 * `./scripts/run-server.sh 2`
 * `./scripts/run-proxy.sh 2`

This will initialize all three replica units. After initialization is finished (indicated by "Finished init, running proxy" appearing in the terminal window of each TaoProxy instance), we can run the TaoClient. For this example, we'll be running the client in interactive mode.

In a new terminal, run `java -cp ../lib/commons-math3-3.6.1.jar:../lib/guava-19.0.jar:TaoServer-1.0-SNAPSHOT.jar TaoClient.TaoClient --id 0`. Follow the instructions given by the program to read and write to/from the system.

****
Usage notes:

 - Before running a load test, make sure that the max_client_id configuration property is at least 1 + clients.

