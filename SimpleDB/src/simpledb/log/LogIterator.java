package simpledb.log;

import simpledb.file.Block;
import simpledb.file.Page;

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
   private Page pg = new Page();
   private int currentrec;
   
   /**
    * Creates an iterator for the records in the log file,
    * positioned after the last log record.
    * This constructor is called exclusively by
    * {@link LogMgr#iterator()}.
    */
   LogIterator(Block blk) {
      // TODO: buf.pin(blk)
      this.blk = blk;
      pg.read(blk);
      // TODO: buf.getInt()
      currentrec = pg.getInt(LogMgr.LAST_POS);
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
      currentrec = pg.getInt(currentrec);
      return new BasicLogRecord(pg, currentrec+INT_SIZE);
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
      pg.read(blk);
      // TODO: buf.getInt()
      currentrec = pg.getInt(LogMgr.LAST_POS);
   }
}
