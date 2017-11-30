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
   /**
    * Replaced page with buffer
    * - private Page pg = new Page();
    * + private Buffer buf;
    *
    * @author Leonard
    */
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
      /*
       * Replaced page read with its buffer equivalent, pin() and then swapped to the buffer's getInt function
       * - pg.read(blk);
       * - currentrec = pg.getInt(LogMgr.LAST_POS);
       * + buf = bufferMgr.pin(blk);
       * + currentrec = buf.getInt(LogMgr.LAST_POS);
       *
       * @author Leonard
       */
      buf = bufferMgr.pin(blk);
      currentrec = buf.getInt(LogMgr.LAST_POS);
   }

   /**
    * Determines if the current log record
    * is the earliest record in the log file.
    *
    * @return true if there is an earlier record
    */
   public boolean hasNext() {
      return currentrec > 0 || blk.number() > 0;
   }

   /**
    * Moves to the next log record in reverse order.
    * If the current log record is the earliest in its block,
    * then the method moves to the next oldest block,
    * and returns the log record from there.
    *
    * @return the next earliest log record
    */
   public BasicLogRecord next() {
      if (currentrec == 0)
         moveToNextBlock();

      /*
       * Converted from page to buffer equivalent
       * - currentrec = pg.getInt(currentrec);
       * - return new BasicLogRecord(pg, currentrec+INT_SIZE);
       * + currentrec = buf.getInt(currentrec);
       * + return new BasicLogRecord(buf, currentrec + INT_SIZE);
       *
       * @author Leonard
       */

      currentrec = buf.getInt(currentrec);
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
      blk = new Block(blk.fileName(), blk.number() - 1);
      /*
       * Converted from page to buffer equivalent. Make sure to unpin buffer here to avoid all buffers being pinned.
       * - pg.read(blk);
       * - currentrec = pg.getInt(LogMgr.LAST_POS);
       * + bufferMgr.unpin(buf);
       * + buf = bufferMgr.pin(blk);
       * + currentrec = buf.getInt(LogMgr.LAST_POS);
       *
       * @author Leonard
       */
      bufferMgr.unpin(buf);
      buf = bufferMgr.pin(blk);
      currentrec = buf.getInt(LogMgr.LAST_POS);
   }
}
