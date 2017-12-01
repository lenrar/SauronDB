package simpledb.buffer;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

import static junit.framework.TestCase.assertEquals;

public class TestBuff {
    @Before
    public void setUp() throws Exception {
        SimpleDB.init("database5");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testPin() {
        System.out.println("testPin start------------------------!");

        Block[] blk1 = new Block[10];
        for (int i = 0; i < 10; i++) {
            blk1[i] = new Block("filename", i);
        }
        BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
        //initially, available buffers should be 8

        basicBufferMgr.showBuffer();

        assertEquals(7, basicBufferMgr.available());

        /*for (int i = 0; i < 10; i++) {

            //pin a block to buffer,if buffer is full, it will wait for some time then fail
            try {
                basicBufferMgr.pin(blk1[i]); //pin a block to buffer
                assertEquals(8 - i - 1, basicBufferMgr.available());
            } catch (BufferAbortException e) {
                System.out.println(i + " buffer pin fails!");//buffer pool is full
            }
        }*/

        System.out.println("testPin end！-------------------------");
        blk1 = null;
    }
}
