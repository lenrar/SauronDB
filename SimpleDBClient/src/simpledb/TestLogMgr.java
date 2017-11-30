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
            LogMgr logMgr = SimpleDB.logMgr();
            File file = new File("filename.txt");
            PrintStream out = new PrintStream(file);
            Block blk = new Block("filename.txt", 1);
            BufferMgr bufferMgr = SimpleDB.bufferMgr();

            Buffer buf = bufferMgr.pin(blk);
            buf.setInt(1, 1, 1, 57);
            bufferMgr.unpin(buf);
            bufferMgr.flushAll(1);

            file.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
