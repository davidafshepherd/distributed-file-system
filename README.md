# Distributed File System
- [src](src) contains the source code:
  - A [DStore](src/Controller.java) (Data Store) stores files. Each file is replicated R times over different Dstores.
  - The [Controller](src/Controller.java) orchestrates client requests and maintains an index with the allocation of files to Dstores, as well as the size in byte of each file. Note that the client gets files directly from Dstores, improving scalability.
