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
            unpinnedBufferBlockList.add(entry.getKey());
         }
      }
      // No unpinned buffer
      if (unpinnedBufferBlockList.size() == 0) {
         return null;
      }
      Block tmp = LRU2(unpinnedBufferBlockList);
      System.out.println("Replace the buffer contains block " + tmp.number() + " in " + tmp.fileName());
      Buffer buff = bufferPoolMap.get(tmp);
      buff.resetAccessTime();
      bufferPoolMap.remove(tmp);
      return buff;
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

      for (int i = 0; i < unpinnedBufferBlockList.size(); i++) {
         Buffer buff = bufferPoolMap.get(unpinnedBufferBlockList.get(i));
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
      }
      Block res;
      if (infiCount >= 1) {
         res = unpinnedBufferBlockList.get(LRUIndex);
      }
      else {
         res = unpinnedBufferBlockList.get(LRU2Index);
      }
      return res;
   }


   public synchronized void showBuffer() {
      for (Block blk : bufferPoolMap.keySet()) {

         System.out.println("=======================");
         System.out.println("Show block in buffer");
         System.out.println("Filename: " + blk.fileName());
         System.out.println("Block Num: " + blk.number());
         System.out.println("Is pinned? : " + bufferPoolMap.get(blk).isPinned());
      }
   }
}
