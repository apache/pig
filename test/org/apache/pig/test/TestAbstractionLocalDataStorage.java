package org.apache.pig.test;

import java.io.OutputStream;
import java.io.IOException;
import java.io.File;
import java.util.Random;
import java.util.Iterator;
import java.io.InputStream;
import java.util.Properties;

import org.junit.Test;
import junit.framework.TestCase;

import org.apache.pig.backend.local.datastorage.*;


import org.apache.pig.backend.datastorage.*;


public class TestAbstractionLocalDataStorage extends TestCase {
        
    private static Random randGen = new Random(17);
    
    private void createDeleteDir(DataStorage ds) throws Throwable {
        final String CONTAINER_NAME = "test_create_del_dir" +
                                      new Integer(randGen.nextInt());
        boolean exception = false;

        // create a container (a dir in the case of hadoop)
        DataStorageContainerDescriptor container = 
            ds.asContainer(CONTAINER_NAME);
        assertTrue(container != null);
        
        OutputStream outImmutable = container.create();

        assertTrue(container.exists());
        assertTrue((new File(CONTAINER_NAME)).exists());
        
        assertTrue(outImmutable instanceof ImmutableOutputStream);
        exception = false;
        try {
            outImmutable.write(new byte[]{1, 2, 3});
        }
        catch (IOException e) {
            exception = true;
        }
        assertTrue(exception);
                
        container.delete();
        
        assertFalse(container.exists());
        assertFalse((new File(CONTAINER_NAME)).exists());
    }
    
    private void createFile(DataStorage ds) throws Throwable {
        final String TEST_CONTAINER = "test_create_file" +
                                      new Integer(randGen.nextInt());
        final String TEST_ELEMENT = "test_file" +
                                    new Integer(randGen.nextInt());
        boolean exception = false;    
        
        // create container
        DataStorageContainerDescriptor container = ds.asContainer(TEST_CONTAINER);
        assertTrue(container != null);
        
        container.create(null);
        assertTrue(container.exists());
        
        assertTrue((new File(TEST_CONTAINER)).exists());
        assertFalse((new File(TEST_CONTAINER)).isFile());
        
        // create element in container
        DataStorageElementDescriptor element = ds.asElement(TEST_CONTAINER,
                                                            TEST_ELEMENT);
        assertTrue(element != null);
     
        // open container to write to it
        OutputStream out = element.create();
        assertTrue(out != null);

        assertTrue((new File(TEST_CONTAINER, TEST_ELEMENT)).exists());
        assertTrue(element.exists());
        
        // write to file
        exception = false;
        byte[] msg = new byte[]{1, 2, 3, 4, 5, 6};
        try {
            out.write(msg);
            out.close();
        }
        catch (Exception e) {
            exception = true;
        }
        assertFalse(exception);
        
        // check file size
        assertTrue((new File(TEST_CONTAINER, TEST_ELEMENT)).length()
                   == msg.length);
        
        Properties fileProps = element.getStatistics();
        
        assertTrue(fileProps != null);
        
        String lengthStr = fileProps.getProperty(DataStorageElementDescriptor.LENGTH_KEY);
        assertTrue(new Integer(lengthStr).intValue() == msg.length);
        
        // check file content - non seek-able input stream
        exception = false;
        try {
            InputStream in = element.open();
            assert(in != null);
            
            byte[] inBytes = new byte[2 * msg.length];
            
            assertTrue(in.read(inBytes) == msg.length);

            for (int i = 0; i < msg.length; ++i) {
                assertTrue(inBytes[ i ] == msg[ i ]);
            }
            
            in.close();
        }
        catch (Exception e) {
            exception = true;
        }
        assertFalse(exception);

        // check file content - seek-able input stream
        exception = false;
        try {
            SeekableInputStream sin = element.sopen();
            
            long pos = sin.tell();
            
            assertTrue(pos == 0);

            for (int i = 0; i < msg.length; ++i) {
                assertTrue(sin.read() == msg[ i ]);                
                assertTrue(sin.tell() == i + 1);
            }

            assertTrue(sin.read() == -1);
         
            // seek at the beginning
            int curPos = 0;
            sin.seek(0, SeekableInputStream.FLAGS.SEEK_SET);
            assertTrue(sin.tell() == curPos);
            assertTrue(sin.read() == msg[ curPos ]);
            ++curPos;
            
            // seek 2 places ahead from cur position
            sin.seek(2, SeekableInputStream.FLAGS.SEEK_CUR);
            assertTrue(sin.tell() == curPos + 2);
            assertTrue(sin.read() == msg[ curPos + 2]);
            curPos += 2;
            
            // seek one place past the end
            sin.seek(-1, SeekableInputStream.FLAGS.SEEK_END);
            assertTrue(sin.tell() == msg.length - 1);
            assertTrue(sin.read() == msg[ msg.length - 1]);
            
            sin.close();
        }
        catch (Exception e) {
            exception = true;
        }
        assertFalse(exception);

        // clean up
        element.delete();
        assertFalse(element.exists());
        assertFalse((new File(TEST_CONTAINER, TEST_ELEMENT)).exists());
        
        assertTrue((new File(TEST_CONTAINER)).delete());
        assertFalse((new File(TEST_CONTAINER)).exists());
        assertFalse(container.exists());
    }

    private void checkDirContent(DataStorageContainerDescriptor container,
                                 int expectedCount,
                                 int numExpectedFiles) {
        Iterator<DataStorageElementDescriptor> iter = container.iterator();
        int count = 0;
        int numFoundFiles = 0;
        
        while (iter.hasNext()) {
            DataStorageElementDescriptor curElem = iter.next();
                        
            ++count;
            
            if (curElem instanceof LocalFile) {
                ++numFoundFiles;
            }
            
        }
        assertTrue(count == expectedCount);
        assertTrue(numFoundFiles == numExpectedFiles);        
    }
    
    @Test
    public void listFiles(DataStorage ds) throws Throwable {
        final String CONTAINER1 = "test_list1";
        final String CONTAINER2 = "test_list2";
        final String ELEMENT1 = "list1";
        final String ELEMENT2 = "list2";
        
        // create dir structure        
        DataStorageContainerDescriptor container1 = ds.asContainer(CONTAINER1);
        assertTrue(container1 != null);
        
        DataStorageElementDescriptor element1 = ds.asElement(CONTAINER1, ELEMENT1);
        assertTrue(element1 != null);
        
        DataStorageContainerDescriptor container2 = ds.asContainer(container1, CONTAINER2);
        assertTrue(container2 != null);
                
        DataStorageElementDescriptor element2 = ds.asElement(container2, ELEMENT2);
        assertTrue(element2 != null);
        
        container1.create().close();
        assertTrue(container1.exists());
        assertTrue((new File(CONTAINER1)).exists());
        assertFalse((new File(CONTAINER1)).isFile());

        element1.create().close();
        assertTrue(element1.exists());        
        assertTrue((new File(CONTAINER1, ELEMENT1)).exists());
        assertTrue((new File(CONTAINER1, ELEMENT1)).isFile());
        
        container2.create().close();
        assertTrue(container2.exists());
        assertTrue((new File(CONTAINER1, CONTAINER2)).exists());
        assertFalse((new File(CONTAINER1, CONTAINER2)).isFile());
        
        
        element2.create().close();
        assertTrue(element2.exists());

        assertTrue((new File(new File(CONTAINER1, CONTAINER2), ELEMENT2)).exists());
        assertTrue((new File(new File(CONTAINER1, CONTAINER2), ELEMENT2)).isFile());

        // iterate on container1
        int expectedCount = 2;
        int numExpectedFiles = 1;
        checkDirContent(container1, expectedCount, numExpectedFiles);
        
        // iterate on container2
        expectedCount = 1;
        numExpectedFiles = 1;
        checkDirContent(container2, expectedCount, numExpectedFiles);        

        // test setting working dir
        DataStorageContainerDescriptor userDir = ds.getActiveContainer();
        
        assertTrue(container1.exists());
        ds.setActiveContainer(container1);
        assertFalse(container1.exists());
        
        ds.setActiveContainer(userDir);
        assertTrue(container1.exists());
        
        // clean-up
        element2.delete();
        assertFalse(element2.exists());
        
        container2.delete();
        assertFalse(container2.exists());
        
        element1.delete();
        assertFalse(element1.exists());
        
        container1.delete();
        assertFalse(container1.exists());
    }
    
    private byte[] initMsg() {
        final int MSG_SIZE = 8 * 1024;
        byte[] msg = new byte[ MSG_SIZE ];
        
        for (int i = 0; i < MSG_SIZE; ++i) {
            msg[ i ] = 'M';
        }
        
        return msg;
    }
    
    private void copyFiles1(DataStorage ds) throws Throwable {
        final String ELEM1 = "test_copy_file1";
        final String ELEM2 = "test_copy_file2";
        
        DataStorageElementDescriptor elem1 = ds.asElement(ELEM1);
        DataStorageElementDescriptor elem2 = ds.asElement(ELEM2);

        OutputStream out = elem1.create();
        byte[] msg = initMsg();
        
        out.write(msg);
        out.close();
        
        elem1.copy(elem2, true);
        assertTrue(elem2.exists());
        assertTrue((new File(ELEM2)).exists());
        
        Properties fileProps = elem2.getStatistics();
        assertTrue(fileProps != null);
        
        String lengthStr = fileProps.getProperty(DataStorageElementDescriptor.LENGTH_KEY);
        assertTrue(new Integer(lengthStr).intValue() == msg.length);        
        
        // clean up
        assertFalse((new File(ELEM1)).exists());
        
        elem2.delete();
        assertFalse(elem2.exists());
    }
    
    private void copyFiles2(DataStorage ds) throws Throwable {
        final String FILE = "test_copy_2";
        final String DIR = "test_copy_2_dir";
        
        DataStorageElementDescriptor elem1 = ds.asElement(FILE);
        DataStorageElementDescriptor elem2 = ds.asElement(DIR, FILE);

        elem1.create().close();
        assertTrue(elem1.exists());
        assertTrue((new File(FILE)).exists());
        
        DataStorageContainerDescriptor container = ds.asContainer(DIR);
        
        container.create();
        assertTrue(container.exists());
        assertTrue((new File(DIR)).exists());
      
        elem1.copy(container, false);
        assertTrue(elem2.exists());
        assertTrue((new File(DIR, FILE)).exists());        
                
        // clean up
        elem1.delete();
        assertFalse(elem1.exists());
        assertFalse((new File(FILE)).exists());
        
        elem2.delete();
        assertFalse(elem2.exists());
        assertFalse((new File(DIR, FILE)).exists());
        
        container.delete();
        assertFalse(container.exists());
        assertFalse((new File(DIR)).exists());
    }

    private void copyDir(DataStorage ds) throws Throwable {
        final String DIR1 = "test_copy_dir1";
        final String DIR11 = "test_copy_dir11";
        final String DIR12 = "test_copy_dir12";
        final String FILE1 = "test_copy_dir_file1";
        final String FILE11 = "test_copy_dir_file11";
        final String COPY_DIR = "test_copy_dir2";
        
        DataStorageContainerDescriptor container1 = ds.asContainer(DIR1);
        DataStorageContainerDescriptor container11 = ds.asContainer(container1, DIR11);
        DataStorageContainerDescriptor container12 = ds.asContainer(container1, DIR12);
        DataStorageElementDescriptor elem1 = ds.asElement(container1, FILE1);
        DataStorageElementDescriptor elem11 = ds.asElement(container11, FILE11);

        DataStorageContainerDescriptor copyContainer = ds.asContainer(COPY_DIR);
        
        container1.create();
        assertTrue(container1.exists());
                
        container11.create();
        assertTrue(container11.exists());
        
        container12.create();
        assertTrue(container12.exists());
        
        elem1.create().close();
        assertTrue(elem1.exists());
        
        elem11.create().close();
        
        container1.copy(copyContainer, true);
        
        assertTrue((new File(COPY_DIR)).exists());
        assertTrue((new File(COPY_DIR, DIR11)).exists());
        assertTrue((new File(COPY_DIR, FILE1)).exists());
        assertTrue((new File(new File(COPY_DIR, DIR11), FILE11)).exists());
        
        // clean up
        // this should have been removed because we set removeSrc flag on for copy op
        assertFalse(elem11.exists());        
        assertFalse(elem11.exists());
        assertFalse(container11.exists());
        assertFalse(container12.exists());
        assertFalse(container1.exists());
        
        // other dirs/files need to be removed
        assertTrue((new File(new File(COPY_DIR, DIR11), FILE11)).delete());
        assertTrue((new File(COPY_DIR, FILE1)).delete());
        assertTrue((new File(COPY_DIR, DIR12)).delete());
        assertTrue((new File(COPY_DIR, DIR11)).delete());
        assertTrue((new File(COPY_DIR)).delete());
    }

    @Test
    public void testLocalDataStorage() throws Throwable {
    	DataStorage ds = new LocalDataStorage();
    	assertTrue (ds != null);
		
		createDeleteDir(ds);
		createFile(ds);
		listFiles(ds);
		copyFiles1(ds);
		copyFiles2(ds);
		copyDir(ds);
    }
}



