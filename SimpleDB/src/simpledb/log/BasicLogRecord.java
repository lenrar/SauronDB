package simpledb.log;

import simpledb.buffer.Buffer;

import static simpledb.file.Page.INT_SIZE;
import static simpledb.file.Page.STR_SIZE;

/**
 * A class that provides the ability to read the values of
 * a log record.
 * The class has no idea what values are there.
 * Instead, the methods {@link #nextInt() nextInt}
 * and {@link #nextString() nextString} read the values 
 * sequentially.
 * Thus the client is responsible for knowing how many values
 * are in the log record, and what their types are.
 * @author Edward Sciore
 */
public class BasicLogRecord {
   // TODO: Convert this to a buffer
//   private Page pg;
   private Buffer buf;
   private int pos;
   
   /**
    * A log record located at the specified position of the specified page.
    * This constructor is called exclusively by
    * {@link LogIterator#next()}.
    * @param buf the page containing the log record
    * @param pos the position of the log record 
    */
   public BasicLogRecord(Buffer buf, int pos) {
      // TODO: Instead this needs to take in a buffer, and set a buffer
//      this.pg = pg;
      this.buf = buf;
      this.pos = pos;
   }
   
   /**
    * Returns the next value of the current log record, 
    * assuming it is an integer.
    * @return the next value of the current log record
    */
   public int nextInt() {
      // TODO: Buffer.getInt(pos)
//      int result = pg.getInt(pos);
      int result = buf.getInt(pos);
      pos += INT_SIZE;
      return result;
   }
   
   /**
    * Returns the next value of the current log record, 
    * assuming it is a string.
    * @return the next value of the current log record
    */
   public String nextString() {
      // TODO: Buffer.getString
//      String result = pg.getString(pos);
      String result = buf.getString(pos);
      pos += STR_SIZE(result.length());
      return result;
   }
}
