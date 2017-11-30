package simpledb;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

import java.io.File;
import java.io.PrintStream;

public class TestLogMgr {
   public static void main(String[] args) {
      try {
         // analogous to the driver
         SimpleDB.init("simpleDB");
         File file = new File("filename.txt");
         PrintStream out = new PrintStream(file);
         out.println(22);
         Block blk = new Block("filename.txt", 1);
         BufferMgr bufferMgr = SimpleDB.bufferMgr();
         LogMgr logMgr = SimpleDB.logMgr();

         Buffer buf = bufferMgr.pin(blk);
         buf.setInt(1, 1, 1, 1);
         bufferMgr.unpin(buf);
         bufferMgr.flushAll(1);

         logMgr.printLogPageBuffer();
         file.delete();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
