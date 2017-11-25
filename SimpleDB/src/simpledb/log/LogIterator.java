package simpledb.log;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

import java.util.Iterator;

import static simpledb.file.Page.INT_SIZE;

/**
 * A class that provides the ability to move through the
 * records of the log file in reverse order.
 * 
 * @author Edward Sciore
 */
class LogIterator implements Iterator<BasicLogRecord> {
   private Block blk;
   // TODO:  This should be a buffer
//   private Page pg = new Page();
   private Buffer buf;
   private BufferMgr bufferMgr = SimpleDB.bufferMgr();
   private int currentrec;
   
   /**
    * Creates an iterator for the records in the log file,
    * positioned after the last log record.
    * This constructor is called exclusively by
    * {@link LogMgr#iterator()}.
    */
   LogIterator(Block blk) {
      this.blk = blk;
      // TODO: buf.pin(blk)
//      pg.read(blk);
      buf = bufferMgr.pin(blk);
      // TODO: buf.getInt()
//      currentrec = pg.getInt(LogMgr.LAST_POS);
      currentrec = buf.getInt(LogMgr.LAST_POS);
   }
   
   /**
    * Determines if the current log record
    * is the earliest record in the log file.
    * @return true if there is an earlier record
    */
   public boolean hasNext() {
      return currentrec>0 || blk.number()>0;
   }
   
   /**
    * Moves to the next log record in reverse order.
    * If the current log record is the earliest in its block,
    * then the method moves to the next oldest block,
    * and returns the log record from there.
    * @return the next earliest log record
    */
   public BasicLogRecord next() {
      if (currentrec == 0) 
         moveToNextBlock();
      // TODO: buf.getInt()
//      currentrec = pg.getInt(currentrec);
      currentrec = buf.getInt(currentrec);
      //TODO: BasicLogRecord(buf)
//      return new BasicLogRecord(pg, currentrec+INT_SIZE);
      return new BasicLogRecord(buf, currentrec + INT_SIZE);

   }
   
   public void remove() {
      throw new UnsupportedOperationException();
   }
   
   /**
    * Moves to the next log block in reverse order,
    * and positions it after the last record in that block.
    */
   private void moveToNextBlock() {
      blk = new Block(blk.fileName(), blk.number()-1);
      // TODO: buf.pin(blk)
//      pg.read(blk);
      bufferMgr.unpin(buf);
      buf = bufferMgr.pin(blk);
      // TODO: buf.getInt()
//      currentrec = pg.getInt(LogMgr.LAST_POS);
      currentrec = buf.getInt(LogMgr.LAST_POS);
   }
}
