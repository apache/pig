package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.VisitorException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.ParseException;

import junit.framework.TestCase;

public class TestBestFitCast extends TestCase {
    private PigServer pigServer;
    private MiniCluster cluster = MiniCluster.buildCluster();
    private File tmpFile;
    
    public TestBestFitCast() throws ExecException, IOException{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
//        pigServer = new PigServer(ExecType.LOCAL);
        int LOOP_SIZE = 20;
        tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        long l = 0;
        for(int i = 1; i <= LOOP_SIZE; i++) {
            ps.println(l + "\t" + i);
        }
        ps.close();
    }
    
    @Before
    public void setUp() throws Exception {
        
    }

    @After
    public void tearDown() throws Exception {
    }
    
    public static class UDF1 extends EvalFunc<Tuple>{
        /**
         * java level API
         * @param input expects a single numeric DataAtom value
         * @param output returns a single numeric DataAtom value, cosine value of the argument
         */
        @Override
        public Tuple exec(Tuple input) throws IOException {
            return input;
        }

        /* (non-Javadoc)
         * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
         */
        @Override
        public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
            List<FuncSpec> funcList = new ArrayList<FuncSpec>();
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.FLOAT),new Schema.FieldSchema(null, DataType.FLOAT)))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.LONG),new Schema.FieldSchema(null, DataType.DOUBLE)))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.FLOAT))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.DOUBLE))));
            /*funcList.add(new FuncSpec(DoubleMax.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DOUBLE)));
            funcList.add(new FuncSpec(FloatMax.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.FLOAT)));
            funcList.add(new FuncSpec(IntMax.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER)));
            funcList.add(new FuncSpec(LongMax.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.LONG)));
            funcList.add(new FuncSpec(StringMax.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.CHARARRAY)));*/
            return funcList;
        }    

    }
    
    @Test
    public void test1() throws Exception{
        //Passing (long, int)
        //Possible matches: (float, float) , (long, double)
        //Chooses (long, double) as it has only one cast compared to two for (float, float)
        pigServer.registerQuery("A = LOAD 'file:" + tmpFile + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "(x,y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals(true,((Tuple)t.get(1)).get(0) instanceof Long);
            assertEquals(true,((Tuple)t.get(1)).get(1) instanceof Double);
            ++cnt;
        }
        assertEquals(20, cnt);
    }
    
    @Test
    public void test2() throws Exception{
        //Passing (int, int)
        //Possible matches: (float, float) , (long, double)
        //Throws Exception as ambiguous definitions found
        pigServer.registerQuery("A = LOAD 'file:" + tmpFile + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "(y,y);");
        try{
            pigServer.openIterator("B");
        }catch (Exception e) {
            String msg = e.getMessage();
            assertEquals(true,msg.contains("as multiple or none of them fit"));
        }
        
    }
    
    @Test
    public void test3() throws Exception{
        //Passing (int, int)
        //Possible matches: (float, float) , (long, double)
        //Chooses (float, float) as both options lead to same score and (float, float) occurs first.
        pigServer.registerQuery("A = LOAD 'file:" + tmpFile + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "((float)y,(float)y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals(true,((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals(true,((Tuple)t.get(1)).get(1) instanceof Float);
            ++cnt;
        }
        assertEquals(20, cnt);
    }
    
    @Test
    public void test4() throws Exception{
        //Passing (long)
        //Possible matches: (float), (integer), (double)
        //Chooses (float) as it leads to a better score that to (double)
        pigServer.registerQuery("A = LOAD 'file:" + tmpFile + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "(x);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals(true,((Tuple)t.get(1)).get(0) instanceof Float);
            ++cnt;
        }
        assertEquals(20, cnt);
    }
    
    @Test
    public void test5() throws Exception{
        //Passing bytearrays
        //Possible matches: (float, float) , (long, double)
        //Throws exception since more than one funcSpec and inp is bytearray
        pigServer.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pigServer.registerQuery("B = FOREACH A generate $0, " + UDF1.class.getName() + "($1,$1);");
        try{
            pigServer.openIterator("B");
        }catch (Exception e) {
            String msg = e.getMessage();
            assertEquals(true,msg.contains("multiple of them were found to match"));
        }
        
    }
}
