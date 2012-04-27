package org.apache.pig.builtin.mock;

import static junit.framework.Assert.*;
import static org.apache.pig.builtin.mock.Storage.*;

import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

public class TestMockStorage {

  @Test
  public void testMockStoreAndLoad() throws Exception {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    Data data = resetData(pigServer);

    data.set("foo",
        tuple("a"),
        tuple("b"),
        tuple("c")
        );

    pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
    pigServer.registerQuery("STORE A INTO 'bar' USING mock.Storage();");

    List<Tuple> out = data.get("bar");
    assertEquals(tuple("a"), out.get(0));
    assertEquals(tuple("b"), out.get(1));
    assertEquals(tuple("c"), out.get(2));
  }
  
  @Test
  public void testMockSchema() throws Exception {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    Data data = resetData(pigServer);

    data.set("foo", "blah:chararray",
        tuple("a"),
        tuple("b"),
        tuple("c")
        );

    pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
    pigServer.registerQuery("B = FOREACH A GENERATE blah as a, blah as b;");
    pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

    assertEquals(schema("a:chararray,b:chararray"), data.getSchema("bar"));
    
    List<Tuple> out = data.get("bar");
    assertEquals(tuple("a", "a"), out.get(0));
    assertEquals(tuple("b", "b"), out.get(1));
    assertEquals(tuple("c", "c"), out.get(2));
  }

}
