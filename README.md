# Distributed File System
- [src](src) contains the source code:
  - A [DStore](src/Controller.java) (Data Store) stores files. Each file is replicated R times over different Dstores.
  - The [Controller](src/Controller.java) orchestrates client requests and maintains an [index](src/IndexEntry.java) with the allocation of files to Dstores as well as the size in byte of each file. Note that the client gets files directly from Dstores, improving scalability.
#
The system supports the following concurrent file operations:
- **JOIN:** Registers a new data store with the controller so it can participate in file storage and replication.
- **STORE:** Stores a file by replicating it across multiple data stores and updating the controller’s index.
- **LOAD:** Retrieves a file by directing the client to a data store holding a replica, with retries on failure.
- **REMOVE:** Deletes a file from all data stores and removes its metadata from the controller.
- **LIST:** Returns the list of files currently stored and fully available in the system.
- **REBALANCE:** Redistributes files across data stores to maintain replication and balance after joins or failures.
