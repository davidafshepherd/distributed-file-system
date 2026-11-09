# Distributed File System
- [src](src) contains the source code:
  - A [Dstore](src/Dstore.java) (Data Store) stores and serves files.   
  - Each file is replicated *R* times across different Dstores.
  - The [Controller](src/Controller.java):
    - Orchestrates client requests (i.e. STORE, LOAD, REMOVE, LIST).
    - Maintains an [index](src/IndexEntry.java) tracking each file's locations, size in bytes and state.
    - Coordinates file replication and rebalancing to preserve file availability.
  - The client gets the files directly from Dstores, improving scalability.
- [client](client) contains the provided client library and an example test program:
  - [client.jar](client.client.jar) provides the client API for communicating with the Controller and transferring files to and from Dstores.
  - [ClientMain](client/ClientMain.java) uses the client library to demonstrate file operations and concurrent requests.
#
To compile the project, run:

```bash
javac *.java
```

To start the Controller, run:

```bash
java Controller <cport> <R> <timeout> <rebalance_period>
```

Where:
- `cport` is the port the Controller listens on.
- `R` is the number of replicas of each file.
- `timeout` is the timeout in milliseconds.
- `rebalance_period` is the interval between rebalancing operations in seconds.

To start a Dstore, run:

```bash
java Dstore <port> <cport> <timeout> <file_folder>
```

Where:
- `port` is the port the Dstore listens on.
- `cport` is the Controller's port.
- `timeout` is the timeout in milliseconds.
- `file_folder` is the directory where the Dstore stores files.
#
The system supports the following concurrent operations:
- **JOIN:** Registers a new data store with the controller so it can participate in file storage and replication.
- **STORE:** Stores a file by replicating it across multiple data stores and updating the controller’s index.
- **LOAD:** Retrieves a file by directing the client to a data store holding a replica, with retries on failure.
- **REMOVE:** Deletes a file from all data stores and removes its metadata from the controller.
- **LIST:** Returns the list of files currently stored and fully available in the system.
- **REBALANCE:** Redistributes files across data stores to maintain replication and balance after joins or failures.
