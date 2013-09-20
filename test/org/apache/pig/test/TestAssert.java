package org.apache.pig.test;

import static junit.framework.Assert.assertEquals;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;

import java.util.List;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Test;

public class TestAssert {

  /**
   * Verify that ASSERT operator works
   * @throws Exception
   */
  @Test
  public void testPositive() throws Exception {
      PigServer pigServer = new PigServer(ExecType.LOCAL);
      Data data = resetData(pigServer);

      data.set("foo",
              tuple(1),
              tuple(2),
              tuple(3)
              );

      pigServer.setBatchOn();
      pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
      pigServer.registerQuery("ASSERT A BY i > 0;");
      pigServer.registerQuery("STORE A INTO 'bar' USING mock.Storage();");
      
      pigServer.executeBatch();

      List<Tuple> out = data.get("bar");
      assertEquals(3, out.size());
      assertEquals(tuple(1), out.get(0));
      assertEquals(tuple(2), out.get(1));
      assertEquals(tuple(3), out.get(2));
  }
  
  /**
   * Verify that ASSERT operator works
   * @throws Exception
   */
  @Test
  public void testNegative() throws Exception {
      PigServer pigServer = new PigServer(ExecType.LOCAL);
      Data data = resetData(pigServer);

      data.set("foo",
              tuple(1),
              tuple(2),
              tuple(3)
              );

      pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
      pigServer.registerQuery("ASSERT A BY i > 1 , 'i should be greater than 1';");
      
      try {
          pigServer.openIterator("A");
      } catch (FrontendException fe) {
          Assert.assertTrue(fe.getCause().getMessage().contains(
                  "Job terminated with anomalous status FAILED"));
      }
       
  }
}
