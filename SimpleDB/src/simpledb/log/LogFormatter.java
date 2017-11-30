package simpledb.log;

import simpledb.buffer.PageFormatter;
import simpledb.file.Page;

/**
 * Added this entire class, necessary for the pinNew function. This class does nothing because logMgr handles its own
 * formatting.
 *
 * @author Leonard
 */
public class LogFormatter implements PageFormatter {
   @Override
   public void format(Page p) {
      // TODO: Implement this class
   }
}
