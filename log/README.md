### Overview of Log

- Log is a struct containing Records, where it also holds a mutex that services can grab.
- To append a record to a log, you just append to the slice.
- We can read a record given an index, we use that index to look up the record in the slice.
    - If the offset given by the client doesn't exist, we return an nerror saying that the offset doesn't exist.

### Overview of Records

- Records is a struct containg a slice of bytes and an offset to indicate where to start appending data.
- Offset refers to the index of the record in a given Log