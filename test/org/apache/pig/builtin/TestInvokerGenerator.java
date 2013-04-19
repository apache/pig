package org.apache.pig.builtin;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestInvokerGenerator {
	private static PigServer pigServer;
	private static Random r;
	
	@Before
	public void setUp() throws Exception {
		pigServer = new PigServer(ExecType.LOCAL);
		r = new Random(42L);
	}
	
	@Test
	public void testConcat() throws Exception {
	    Data data = resetData(pigServer);

	    Set<Tuple> inputs = ImmutableSet.of(tuple("a"), tuple("b"), tuple("c"));
	    Set<Tuple> expected = Sets.newHashSet();
	    
	    for (Tuple t : inputs) {
	    	String str = (String)t.get(0);
	    	expected.add(tuple(str.concat(str)));
	    }
	    
	    data.set("foo", Utils.getSchemaFromString("x:chararray"), inputs);
		
		pigServer.registerQuery("define concat InvokerGenerator('java.lang.String','concat','String');");
		pigServer.registerQuery("a = load 'foo' using mock.Storage();");
		pigServer.registerQuery("b = foreach @ generate concat($0, $0);");
		pigServer.registerQuery("store b into 'bar' using mock.Storage();");
		
		List<Tuple> results = data.get("bar");
		assertEquals(expected.size(), results.size());
		for (Tuple t : results) {
			assertTrue(expected.remove(t));
		}
		assertEquals(0, expected.size());
	}
	
	@Test
	public void testValueOf() throws Exception {
	    Data data = resetData(pigServer);

	    Set<Tuple> inputs = Sets.newHashSet();
	    while (inputs.size() < 1000) {
	    	inputs.add(tuple(Integer.toString(r.nextInt())));
	    }
	    Set<Tuple> expected = Sets.newHashSet();
	    
	    for (Tuple t : inputs) {
	    	String str = (String)t.get(0);
	    	expected.add(tuple(Integer.valueOf(str)));
	    }
	    
	    data.set("foo", Utils.getSchemaFromString("x:chararray"), inputs);
		
		pigServer.registerQuery("define valueOf InvokerGenerator('java.lang.Integer','valueOf','String');");
		pigServer.registerQuery("a = load 'foo' using mock.Storage();");
		pigServer.registerQuery("b = foreach @ generate valueOf($0);");
		pigServer.registerQuery("store b into 'bar' using mock.Storage();");
		
		List<Tuple> results = data.get("bar");
		assertEquals(expected.size(), results.size());
		for (Tuple t : results) {
			assertTrue(expected.remove(t));
		}
		assertEquals(0, expected.size());		
	}
}