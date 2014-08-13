/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.test.evaluation.xml;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.xml.XPath;
import org.junit.Test;

public class XPathTest {

    @Test
    public void testExecTuple() throws Exception {

        final XPath xpath = new XPath();

        final Tuple tuple = mock(Tuple.class);

        when(tuple.get(0))
                .thenReturn(
                        "<book id=\"bk101\">"
                                + "<author>Gambardella, Matthew</author>"
                                + "<title>XML Developer's Guide</title>"
                                + "<genre>Computer</genre>"
                                + "<price>44.95</price>"
                                + "<publish_date>2000-10-01</publish_date>"
                                + "<description>An in-depth look at creating applications with XML.</description>"
                                + "</book>");

        when(tuple.size()).thenReturn(2);

        when(tuple.get(1)).thenReturn("book/author");
        assertEquals("Gambardella, Matthew", xpath.exec(tuple));

    }

    @Test
    public void testRepeatingCallWithSameXml() throws Exception {

        final XPath xpath = new XPath();

        final Tuple tuple = mock(Tuple.class);

        when(tuple.get(0)).thenReturn(
                        "<book id=\"bk101\">"
                                + "<author>Gambardella, Matthew</author>"
                                + "<title>XML Developer's Guide</title>"
                                + "<genre>Computer</genre>"
                                + "<price>44.95</price>"
                                + "<publish_date>2000-10-01</publish_date>"
                                + "<description>An in-depth look at creating applications with XML.</description>"
                                + "</book>");

        when(tuple.size()).thenReturn(2);

        when(tuple.get(1)).thenReturn("book/author");
        assertEquals("Gambardella, Matthew", xpath.exec(tuple));
        assertNotEquals("Someone else", xpath.exec(tuple));

        when(tuple.get(1)).thenReturn("book/price");
        assertEquals("44.95", xpath.exec(tuple));
        assertNotEquals("00.00", xpath.exec(tuple));
    }

    @Test
    public void testCacheFlag() throws Exception {
        
        final XPath xpath = new XPath();
        
        final Tuple tuple = mock(Tuple.class);
        
        when(tuple.get(0)).thenReturn(
                         "<book id=\"bk101\">" +
                          "<author>Gambardella, Matthew</author>" +
                      "<title>XML Developer's Guide</title>" +
                      "<genre>Computer</genre>" +
                      "<price>44.95</price>" +
                      "<publish_date>2000-10-01</publish_date>" +
                      "<description>An in-depth look at creating applications with XML.</description>" +
                        "</book>");

        when(tuple.size()).thenReturn(3);
        
        //cache on
        when(tuple.get(2)).thenReturn(true);
        
        when(tuple.get(1)).thenReturn("book/author");
        assertEquals("Gambardella, Matthew", xpath.exec(tuple));
        assertNotEquals("Someone else", xpath.exec(tuple));
        
        when(tuple.get(1)).thenReturn("book/price");
        assertEquals("44.95", xpath.exec(tuple));
        assertNotEquals("00.00", xpath.exec(tuple));
        
        //cache off
        when(tuple.get(2)).thenReturn(false);
        
        when(tuple.get(1)).thenReturn("book/author");
        assertEquals("Gambardella, Matthew", xpath.exec(tuple));
        assertNotEquals("Someone else", xpath.exec(tuple));
        
        when(tuple.get(1)).thenReturn("book/price");
        assertEquals("44.95", xpath.exec(tuple));
        assertNotEquals("00.00", xpath.exec(tuple));
                
    }
    
    
    //@Test --optional test
    public void testCacheBenefit() throws Exception{

        final XPath xpath = new XPath();
        
        //should be a live instance this time
        final Tuple tuple = TupleFactory.getInstance().newTuple(3);
        
        //cache on
        tuple.set(2, true);
        final long withCache = timeTheUDF(tuple, xpath);
        
        //cache off
        tuple.set(2, false);
        final long withOutCache = timeTheUDF(tuple, xpath);
        
        System.out.println(withCache + "\t" + withOutCache);
        
        assertTrue(withCache < withOutCache);

    }
    
    private long timeTheUDF(final Tuple tuple, final XPath xpath) throws Exception{
        
        final long start = System.currentTimeMillis();
        
        for(int i = 0; i < 50000; i++){
            
            tuple.set(0,
                    "<book id=\"bk101" + i + "\">" + //we need to make sure xml changes
                          "<author>Gambardella, Matthew</author>" +
                          "<title>XML Developer's Guide</title>" +
                          "<genre>Computer</genre>" +
                          expandXml() + 
                          "<price>44.95</price>" +
                          "<publish_date>2000-10-01</publish_date>" +
                          "<description>An in-depth look at creating applications with XML.</description>" +
                     "</book>");
            
            //caching is used here. for 2nd and 3rd calls to xpath.exec, the cached javax.xml.xpath.XPath should help
            tuple.set(1, "book/author");
            assertEquals("Gambardella, Matthew", xpath.exec(tuple));
            
            tuple.set(1, "book/price");
            assertEquals("44.95", xpath.exec(tuple));
            
            tuple.set(1, "book/publish_date");
            assertEquals("2000-10-01", xpath.exec(tuple));
        }
        
        return System.currentTimeMillis() - start;
    }
    
    private String expandXml() {
        
        final StringBuilder sb = new StringBuilder();
        
        final int max = RandomUtils.nextInt(100);
        
        for(int i = 0; i < max; i++) {
            sb.append("<expansion>This is an expansion of the xml to simulate random sized xml" + i + "</expansion>"); 
        }
        
        return sb.toString();
    }
}
