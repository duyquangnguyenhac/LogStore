

A log holds many segments

A segments hold a config, an index and a store

A index shares information about records in a store using a relative offSet to store the absolute positions in a store.
An index uses a mmaped file to write its index.

A store is a file-backed record to write bytes to.