# FastBufferTree
A fast buffer tree implementation for graph streaming. 

The following describes some of the basics of how this works. There are two key objects `BufferTree` and `BufferControlBlock`.

## BufferTree
This class specifies most of the meta-data and functions for handling a buffer tree. Specifically, the buffer tree maintains a list of nodes that compose the tree in addition to some other buffers used to enable efficient IO when either flushing or reading data from a node.

### Tree Structure
Tree includes parameters `M` the size of a buffer, `B` the branching factor, and finally `N` the number of graph node ids we expect to ingest.

```
           ---root---
         /      |     \
     node       node    node
   /  |  \       |  \     |  \
node node node node node node node
```

Each of these nodes contains a buffer of size 2M and has B children. We construct the tree so that there is a unique node mapping to each of the `N` graph nodes. In the above example B is 3 and N is 7. The bottom level would be full if N equal to 3^2=9.

### Flushing
When a either a root node or an internal node of the tree stores data of size ≥ M then it is ready to be flushed. This flush may happen asynchronously if desired so long as the data stored in the buffer does not exceed 2M.

When flushing we utilize `flush_buffers` to achieve efficient file writing. The data in the node is scanned and, based upon the source node of each update, is placed into the appropriate `flush_buffer` when these buffers become full their contents are written to the corresponding child. The flush buffers are of size equal to a page and therefore IOs should be efficient.

A flush of a leaf node is simply accomplished by adding a 'tag' to the data in question to the `work_queue`. When it is time 


## BufferControlBlock
Encodes the meta-data associated with a block including its `file_offset` and `storage_ptr`. These two attributes represent the location of a node within the large and physically contiguous `backing_store` file. The nodes of the tree are stored in the file following a breadth first search. Specifically, the data stored within the buffer at each node is what is held within the file. Therefore the data in each level is contiguous on disk.

Example:  
```
------------------------------------------------
|Node 1| Node 2| Node 3| Node 4| Node 5| Node 6|
------------------------------------------------
```

This buffer encodes the following tree with `B=2` and `N=4`
```
      ---root---
       /      \      
   node1      node2    
    /  \       /  \  
node3 node4 node5 node6
```

Note that the root does not appear in the backing store. This is because it is stored entirely in RAM. There is also no BufferControlBlock for the root node for the same reason.
