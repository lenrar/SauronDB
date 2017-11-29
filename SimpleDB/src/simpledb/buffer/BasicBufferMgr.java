package simpledb.buffer;

import simpledb.file.Block;
import simpledb.file.FileMgr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Map<Block, Buffer> bufferPoolMap;
   private Buffer[] bufferpool;
   private int numAvailable;
   private int newBuffers;

   /**
    * Creates a buffer manager having the specified number
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferPoolMap = new HashMap<>();
      numAvailable = numbuffs;
      newBuffers = numbuffs;
      /*bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer();*/
   }

   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()) {
         Buffer buff = entry.getValue();
         if (buff.isModifiedBy(txnum))
            buff.flush();
      }
      /*for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush(); // write back to block
         */
   }

   /**
    * Pins a buffer to the specified block.
    * If there is already a buffer assigned to that block
    * then that buffer is used;
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      /**
       * put this mapping into bufferPoolMap
       * update access time
       * @author
       */
      bufferPoolMap.put(blk, buff);
      buff.updateAccessTime();
      return buff;
   }

   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it.
    * Returns null (without allocating the block) if
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();

      bufferPoolMap.put(buff.block(), buff);
      buff.updateAccessTime();

      return buff;
   }

   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }

   /**
    *   Determines whether the map has a mapping from
    *   the block to some buffer.
    *   @paramblk the block to use as a key
    *   @return true if there is a mapping; false otherwise
    */
   synchronized boolean containsMapping (Block blk) {
      return bufferPoolMap.containsKey(blk);
   }

   /**
    *   Returns the buffer that the map maps the specified block to.
    *   @paramblk the block to use as a key
    *   @return the buffer mapped to if there is a mapping; null otherwise */
   synchronized Buffer getMapping (Block blk)   {
      return bufferPoolMap.get(blk);
   }

   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }

   private Buffer findExistingBuffer(Block blk) {
      return getMapping(blk);
      /*for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;*/
   }

   private Buffer chooseUnpinnedBuffer() {

      // If there is a buffer slot available, return this buffer
      if (newBuffers > 0) {
         newBuffers--;
         return new Buffer();
      }

      // No slot available, replace a unpinned buffer
      ArrayList<Block> unpinnedBufferBlockList = new ArrayList<>();
      for (Map.Entry<Block, Buffer> entry : bufferPoolMap.entrySet()) {
         Buffer buff = entry.getValue();
         if (!buff.isPinned()) {
            //bufferPoolMap.remove(entry.getKey());
            unpinnedBufferBlockList.add(entry.getKey());
            //return buff;
         }
      }
      // No unpinned buffer
      if (unpinnedBufferBlockList.size() == 0) {
         return null;
      }
      Block tmp = LRU2(unpinnedBufferBlockList);
      Buffer buff = bufferPoolMap.get(tmp);
      buff.resetAccessTime();
      bufferPoolMap.remove(tmp);
      return buff;

      /*for (Buffer buff : bufferpool)
         if (!buff.isPinned())
         return buff;
      return null;*/
   }

   /**
    * Add a function LRU2()
    * Choose replace buffer by using LRU(K=2)
    * @author
    */
   private Block LRU2(ArrayList<Block> unpinnedBufferBlockList) {
      int infiCount = 0;
      long minLastAccTime = Long.MAX_VALUE;
      long minSecLastAccTime = Long.MAX_VALUE;
      int LRU2Index = -1;
      int LRUIndex = -1;
      /*
      System.out.println(unpinnedBufferBlockList.size());
      System.out.println(minLastAccTime);
      System.out.println(minSecLastAccTime);
      */

      for (int i = 0; i < unpinnedBufferBlockList.size(); i++) {
         Buffer buff = bufferPoolMap.get(unpinnedBufferBlockList.get(i));
         /*
         System.out.println("index: " + i);
         System.out.print("last: " + buff.getLastAccessTime() + " ");
         System.out.println("seclast: " + buff.getSecLastAccessTime());
         */
         if (buff.getSecLastAccessTime() <= minSecLastAccTime) {
            minSecLastAccTime = buff.getSecLastAccessTime();
            LRU2Index = i;
         }
         if (buff.getSecLastAccessTime() == Long.MAX_VALUE) {
            infiCount++;
            if (buff.getLastAccessTime() < minLastAccTime) {
               minLastAccTime = buff.getLastAccessTime();
               LRUIndex = i;
            }
         }

//         if (buff.getLastAccessTime() < minLastAccTime) {
//            minLastAccTime = buff.getLastAccessTime();
//            LRUIndex = i;
//         }
         /*
         System.out.print("minlast: " + minLastAccTime + " ");
         System.out.println("minsec: " + minSecLastAccTime);
         */
      }
      Block res;
      /*
      System.out.println("inficount: " + infiCount);
      System.out.println("LRUIndex: " + LRUIndex);
      System.out.println("LRU2Index: " + LRU2Index);
      */
      if (infiCount >= 1) {
         res = unpinnedBufferBlockList.get(LRUIndex);
      }
      else {
         res = unpinnedBufferBlockList.get(LRU2Index);
      }
      return res;
   }
}
