package simpledb.buffer;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

import static junit.framework.TestCase.assertEquals;


/**
 * Test file of Buffer Manager
 *
 * Testing scenarios
 *
 * 1. Create a list of files-blocks. In this case, we create 10 file blocks
 *
 * 2. Check the number of available buffers initially. All but one should be available as only
 * one of them has been pinned by th e logmgr yet. The initial buffer pool size is 8. After initialization phase,
 * only 7 buffers should be available.
 *
 * 3. Keep pinning buffers one by one and check the number of available buffers.
 *
 * 4. When all buffers have been pinned, if pin request is made again, throw an exception
 *
 * 6. Unpin a few buffers and see if you are still getting an exception or not. Here we unpin buffers contain block 0, 1, 2.
 * Then we pin block 7, 8, 9 again.
 *
 * 7. Try to pin a new buffer again, and check your replacement policy while seeing which currently unpinned buffer is replaced.
 * Now, all buffers in buffer pool have been pined and only been pined once. At this time, we pin block 7, 8, 9 again then unpin
 * them twice. So now according LRU(2), once we pin a new block, the buffer contains block 7 should be replaced from buffer pool.
 * Then we unpin block 3, 4, 5, 6, so their distance should be infinity at this time. Then we should apply LRU to buffers whose
 * distance is infinity. In this case, we should replace block 3 when we pin a new block.
 * 
 * @author Guanxu Yu
 */

public class TestBufferMgr {
    @Before
    public void setUp() throws Exception {
        SimpleDB.init("sampleDatabase");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testPin() {

        BufferMgr bufferMgr = new SimpleDB().bufferMgr();

        int allAvailableBufNum = 7;

        int blocklen = 10;

        Block[] blockList = new Block[blocklen];
        for (int i = 0; i < blocklen; i++) {
            blockList[i] = new Block("testFile", i);
        }

        System.out.println("================== Start Test Available buffer numbers =========================");
        // After initialization stage, logMgr occupies one buffer from buffer pool. So there should be 7 buffers available.
        assertEquals(7, bufferMgr.available());
        System.out.println("Now available buffer number is: " + bufferMgr.available());
        System.out.println("================== End Test Available buffer numbers =========================");
        System.out.println();
        System.out.println("================== Start Test Pin =========================");

        System.out.println("Now try to pin buffers contain block0-9");
        for (int i = 0; i < blocklen; i++) {
            try {
                bufferMgr.pin(blockList[i]);
                assertEquals(allAvailableBufNum-i-1, bufferMgr.available());
            } catch (BufferAbortException e) {
                System.out.println("Pin block" + blockList[i].number() + " in " +blockList[i].fileName() + " fails! ");
            }
        }
        System.out.println("================== End Test Pin =========================");
        System.out.println();
        System.out.println("================== Start Test Unpin =========================");

        // First unpin 3 buffers
        System.out.println("Start unpin buffers contain block0, block1, block2");
        bufferMgr.unpin(bufferMgr.getMapping(blockList[0]));
        bufferMgr.unpin(bufferMgr.getMapping(blockList[1]));
        bufferMgr.unpin(bufferMgr.getMapping(blockList[2]));
        System.out.println("Unpin blocks success!");

        // Then try to pin buffers
        System.out.println("Now try to pin buffers contain block7, block8, block9");
        for (int i = 7; i < blocklen; i++) {

            try {
                bufferMgr.pin(blockList[i]);
            } catch (BufferAbortException e) {
                System.out.println("Pin block" + blockList[i].number() + " fails! ");
            }
        }

        System.out.println("Pin new buffers success! ");

        System.out.println("================== End Test Unpin =========================");
        System.out.println();

        System.out.println("================== Start Test Replacement Policy =========================");

        System.out.println("Now all buffers in buffer pool has been pined and only be pined once");
        System.out.println("Then pin buffers contain blocks 7, 8, 9 again");

        for (int i = 7; i < blocklen; i++) {

            try {
                bufferMgr.pin(blockList[i]);
            } catch (BufferAbortException e) {
                System.out.println("Pin block" + blockList[i].number() +" in " + blockList[i].fileName() + " fails! ");
            }
        }

        System.out.println("Now buffers contain blocks 7,8,9 has been pined twice");
        System.out.println("Next unpin buffers contain blocks 7,8,9 twice");


        for (int j = 0; j < 2; j++) {
            for (int i = 7; i < blocklen; i++) {

                try {
                    bufferMgr.unpin(bufferMgr.getMapping(blockList[i]));
                } catch (BufferAbortException e) {
                    System.out.println("Pin block" + blockList[i].number() + " in " + blockList[i].fileName() + " fails! ");
                }
            }
        }
        System.out.println("Now, according to our replacement policy, if we pin a new buffer now, buffer 7 should be replaced");
        System.out.println();
        System.out.println(" *** Now start pin a new buffer *** ");
        bufferMgr.pin(blockList[0]);

        System.out.println();
        System.out.println("Now buffers contain blocks 3,4,5,6 is pined once, then we unpin all of them");

        for (int i = 3; i < 7; i++) {

            try {
                bufferMgr.unpin(bufferMgr.getMapping(blockList[i]));
            } catch (BufferAbortException e) {
                System.out.println("Pin block" + blockList[i].number() + " fails! ");
            }
        }

        System.out.println("Then we pin a new block. According to our policy, \n the distance of 4 buffers in the buffer pool " +
                "is infinity, \n so in this situation, we should apply LRU to all buffers whose distance is infinity. \n Here when we pin" +
                "a new block, the buffer manager should replace the buffer contains block 3");

        System.out.println();
        System.out.println(" *** Now start pin a new buffer *** ");
        bufferMgr.pin(blockList[1]);

        System.out.println();
        System.out.println("================== End Test Replacement Policy =========================");
    }

}
