################# Simple Implementation of Amazon Dynamo Database ################

### Concept: 
This is a simplefied version of Amazon Dynamo to store key-value based data. It provides Data Partitioning, Data Replication, Concurrent Data Manipulation and handles Node Failures.

### Implementation:
* The application is launched on 5 different AVDs each with a specified port.
* Each application has a server port which is constantly active to receive messages and a client which sends messages to the other AVDs.
* Messages are sent over the devices using TCP protocol.
* The Client and Server ports are implemented using Java Sockets and Java AsycTasks.
* Messages received are stored in a key-value local Map for every AVD.
* Every Data input (key-value) is stored at a specific Node based on the Hash value of its key generated using SHA-1.
* The application handles Data Partitioning, Replication and device failure.
* Data Partitioning is performed by storing the key-value based data using a Hash Table.
* Amazon's Quorum replication is implemented to back up the data at 2 predecessor nodes in the hashed node sequence.
* A failure handling algorithm is implemented to tackle device failures and rejoins. All the data is preserved and restored using the replica Nodes.
* The application also alows concurrent query along with concurent data manipulation on multiple nodes in the system.

### Link to the Code files:
https://github.com/Sumedh0192/Distributed-Systems/tree/master/SimpleDynamo/app/src/main/java/edu/buffalo/cse/cse486586/simpledynamo/

### Link to official project description:
https://github.com/Sumedh0192/Distributed-Systems/blob/master/SimpleDynamo/PA%204%20Specification.pdf
