# SauronDB
An extended fork of SimpleDB (http://www.cs.bc.edu/~sciore/simpledb/).  

# Meet The Team

| Team Member   | Unity Id      |
| :------------- | :------------- |
| Dustin Lambright  | dalambri  |
| Guanxu Yu  | gyu9  |
| Darshan Bhandari | dbhanda |
| Leonard Kerr | lwkerr |
| Yuchen Sun | ysun34 |

# Files Changed - Add these on your own, lads!

## SimpleDB.java
#### initFileLogAndBufferMgr() method
* Updated such that the buffermanager is initialized before the log manager so that the log manager can use the buffer manager to pin blocks.

## LogMgr.java
Changed all methods to now use buffers from the buffer manager instead of pages.
#### printLogPageBuffer() method
* This is the optional print method to view the changes made to the pinned block

## LogFormatter.java
This is a new class which implements PageFormatter to satisfy the need for a PageFormatter in the pinNew method. This is essentially an empty class.

## Buffer.java
#### Add two private variables in Buffer class
* Add `private long lastAccessTime` and `secLastAccessTime` to record last/second last access time of a buffer.
#### getContents() method
* This is a wrapper method to get the contents of the pinned block for the printLogPageBuffer() method
#### updateAccessTime()
* Update access time of buffer when buffer is pinned
#### getLastAccessTime()
* Get last access time of a buffer
#### getSecLastAccessTime()
* Get second last access time of a buffer
#### resetAccessTime()
* Reset last/second last access time to infinity when a buffer is chosen to replace

## BasicBufferMgr.java
#### BasicBufferMgr()
* This is the constructor of BasicBufferMgr class. Since we changed the bufferpoll into map, so we changed how bufferpool initialized at constructor.
#### flushAll() method
* Rewrite this method to fit map structure
#### pin() method
* Rewrite this method to fit map structure and update buffer access time
#### pinNew() method
* Rewrite this method to fit map structure and update buffer access time
#### containsMapping() method
* Used for testing
#### getMapping() method
* Used for testing
#### findExistingBuffer(Block blk) method
* Returns the buffer that the map maps the specified block to.
#### chooseUnpinnedBuffer() method
* Returns the buffer that is unpinned. Change function to work with map
* Choose replacing buffer based on LRU (K=2) policy
#### LRU2() method
* LRU (K=2) policy, take a list of candidate buffers, return chosen one by LRU

# How to run the test scenarios

## Log Manager:
* in lines DUZN and DUZN of LogMgr.java we have hard-coded print statements for the pinned block to prove functionality before and after flush.  If you want to avoid these statements, feel free to comment them out.

* We've written TestLogMgr to show the use of pinned blocks.  If you run TestLogMgr many times, you will see that the block pinned to the buffer increases (Displayed like so: Buffer number pinned to the log block: 14)

## Buffer Manager:
* In TestBufferMgr.java file, we already setup testcase for Buffer Manager. And also we add comments and hard-code print statements to indicate what our testcase looks like. You just need to run it and see the result.
