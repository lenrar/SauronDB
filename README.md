# SauronDB
An extended fork of SimpleDB (http://www.cs.bc.edu/~sciore/simpledb/).  

# Meet The Team

| Team Member   | Unity Id      |
| :------------- | :------------- |
| Dustin Lambright  | dalambri  |
| Guanxu Yu  | gyu9  |
| Darshan Bhandari | ????dbhandari???? |
| Leonard Kerr | ????lwkerr???? |
| Yuchen Sun | ????ysun34???? |

# Files Changed - Add these on your own, lads!

## LogMgr.java
#### printLogPageBuffer() method
```java
public void printLogPageBuffer() {
  System.out.println("----------------------------------------------");
  System.out.println("  Buffer number pinned to the log block: " + mybuf.block().number()); //.getPins());

  ByteBuffer byteBuffer = mybuf.getContents();
  byte[] bb = new byte[byteBuffer.remaining()];
  String s = new String(bb, StandardCharsets.UTF_8);

  System.out.println("  Contents of buffer:    " + s);
  System.out.print("  Values of buffer ints: ");

  for ( int i = 0; i < bb.length; i ++){
     System.out.print(bb[i]);
  }

  System.out.println("");
}
```
## Buffer.java
#### getContents() method
```java
public ByteBuffer getContents(){
    return contents.getContents();
}
```


# How to run the test scenarios

* Run these files:
