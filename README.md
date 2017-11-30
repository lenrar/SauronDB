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

## LogMgr.java
#### printLogPageBuffer() method
* This is the optional print method to view the changes made to the pinned block

## Buffer.java
#### getContents() method
* This is a wrapper method to get the contents of the pinned block for the printLogPageBuffer() method



# How to run the test scenarios

## Log Manager:
* in lines DUZN and DUZN of LogMgr.java we have hard-coded print statements for the pinned block to prove functionality before and after flush.  If you want to avoid these statements, feel free to comment them out.

* We've written TestLogMgr to show the use of pinned blocks.  If you run TestLogMgr many times, you will see that the block pinned to the buffer increases (Displayed like so: Buffer number pinned to the log block: 14)
