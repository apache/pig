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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.xml.XPathAll;
import org.junit.Ignore;
import org.junit.Test;

public class XPathAllTest {
    
    @Test
    public void testExecTuple() throws Exception {

        final XPathAll xpath = new XPathAll();

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
        when(tuple.get(1)).thenReturn("book");
        
        assertEquals(
                "Gambardella, Matthew, XML Developer's Guide, Computer, 44.95, 2000-10-01, An in-depth look at creating applications with XML.",
                xpath.exec(tuple).get(0));
    }

    @Test
    public void testExecTupleWithInnerNodes() throws Exception {

        final XPathAll xpath = new XPathAll();

        final Tuple tuple = mock(Tuple.class);

        when(tuple.get(0)).thenReturn(
                "<book id=\"bk101\">" 
                		+ "<authors>" 
	                		+ "<author_1>Gambardella</author_1>"
	                        + "<author_2>Matthew</author_2>" 
	                		+ "<author_2>Mike</author_2>" 
                        + "</authors>"
                        + "<title>XML Developer's Guide</title>" 
                        + "<genre>Computer</genre>" 
                        + "<price>44.95</price>"
                        + "<publish_date>2000-10-01</publish_date>"
                        + "<description>An in-depth look at creating applications with XML.</description>" 
                + "</book>");

        when(tuple.size()).thenReturn(2);
        when(tuple.get(1)).thenReturn("book/authors");
        
        assertEquals("Gambardella, Matthew, Mike", xpath.exec(tuple).get(0));
    }

    @Test
    public void testExecTupleWithMultipleParentNodes() throws Exception {

        final XPathAll xpath = new XPathAll();

        final Tuple tuple = mock(Tuple.class);

        when(tuple.get(0)).thenReturn(
                "<bookstore>"
                + "<book id=\"bk101\">" 
                        + "<authors>" 
                            + "<author_1>Gambardella</author_1>"
                            + "<author_2>Matthew</author_2>" 
                            + "<author_3>Mike</author_3>"
                        + "</authors>"
                        + "<title>XML Developer's Guide</title>" 
                        + "<genre>Computer</genre>" 
                        + "<price>44.95</price>"
                        + "<publish_date>2000-10-01</publish_date>"
                        + "<description>An in-depth look at creating applications with XML.</description>"
                + "</book>"
                + "<book id=\"bk102\">"
                    + "<authors>"
                        + "<author_1>Kent</author_1>"
                        + "<author_2>Beck</author_2>" 
                    + "</authors>"
                    + "<title>HTML Developer's</title>"
                    + "<genre>Computer</genre>" 
                    + "<price>60.00</price>"
                    + "<publish_date>2000-10-01</publish_date>"
                    + "<description>An in-depth look at creating applications with HTML.</description>"
                + "</book>"
          + "</bookstore>");

        when(tuple.size()).thenReturn(4);
        when(tuple.get(2)).thenReturn(true);
        when(tuple.get(3)).thenReturn(true);
        
        when(tuple.get(1)).thenReturn("bookstore/book");
        assertEquals(2, xpath.exec(tuple).getAll().size());
        assertEquals(
             ", XML Developer's Guide, Computer, 44.95, 2000-10-01, An in-depth look at creating applications with XML.",
             xpath.exec(tuple).get(0));
        assertEquals(
                ", HTML Developer's, Computer, 60.00, 2000-10-01, An in-depth look at creating applications with HTML.",
                xpath.exec(tuple).get(1));
        
        when(tuple.get(1)).thenReturn("bookstore/book/authors");
        assertEquals(2, xpath.exec(tuple).getAll().size());
        assertEquals("Gambardella, Matthew, Mike", xpath.exec(tuple).get(0));
        assertEquals("Kent, Beck", xpath.exec(tuple).get(1));
     
    }

    @Test
    public void testRepeatingCallWithSameXml() throws Exception {

        final XPathAll xpath = new XPathAll();

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
        assertEquals("Gambardella, Matthew", xpath.exec(tuple).get(0));
        assertNotEquals("Someone else", xpath.exec(tuple).get(0));

        when(tuple.get(1)).thenReturn("book/price");
        assertEquals("44.95", xpath.exec(tuple).get(0));
        assertNotEquals("00.00", xpath.exec(tuple).get(0));

        when(tuple.get(1)).thenReturn("book/genre");
        assertEquals("Computer", xpath.exec(tuple).get(0));
        assertNotEquals("Sometihng else", xpath.exec(tuple).get(0));
    }

    @Test
    public void testCacheFlag() throws Exception {

        final XPathAll xpath = new XPathAll();

        final Tuple tuple = mock(Tuple.class);

        when(tuple.get(0)).thenReturn(
                "<book id=\"bk101\">" + "<author>Gambardella, Matthew</author>"
                        + "<title>XML Developer's Guide</title>" + "<genre>Computer</genre>" + "<price>44.95</price>"
                        + "<publish_date>2000-10-01</publish_date>"
                        + "<description>An in-depth look at creating applications with XML.</description>" + "</book>");

        when(tuple.size()).thenReturn(3);

        // cache on
        when(tuple.get(2)).thenReturn(true);

        when(tuple.get(1)).thenReturn("book/author");
        assertEquals("Gambardella, Matthew", xpath.exec(tuple).get(0));
        assertNotEquals("Someone else", xpath.exec(tuple).get(0));

        when(tuple.get(1)).thenReturn("book/price");
        assertEquals("44.95", xpath.exec(tuple).get(0));
        assertNotEquals("00.00", xpath.exec(tuple).get(0));

        // cache off
        when(tuple.get(2)).thenReturn(false);

        when(tuple.get(1)).thenReturn("book/author");
        assertEquals("Gambardella, Matthew", xpath.exec(tuple).get(0));
        assertNotEquals("Someone else", xpath.exec(tuple).get(0));

        when(tuple.get(1)).thenReturn("book/price");
        assertEquals("44.95", xpath.exec(tuple).get(0));
        assertNotEquals("00.00", xpath.exec(tuple).get(0));

    }

    @Ignore
    @Test    
    public void testCacheBenefit() throws Exception {

        final XPathAll xpath = new XPathAll();

        // should be a live instance this time
        final Tuple tuple = TupleFactory.getInstance().newTuple(3);

        // cache on
        tuple.set(2, true);
        final long withCache = timeTheUDF(tuple, xpath);

        // cache off
        tuple.set(2, false);
        final long withOutCache = timeTheUDF(tuple, xpath);

        System.out.println(withCache + "\t" + withOutCache);

        assertTrue(withCache < withOutCache);

    }

    @Test
    public void testExecTupleWithSimpleNamespace() throws Exception {

        final XPathAll xpath = new XPathAll();

        final Tuple tuple = mock(Tuple.class);

        when(tuple.get(0)).thenReturn(
                "<ann:book id=\"bk101\">" 
                		+ "<author>Gambardella, Matthew</author>"
                        + "<title>XML Developer's Guide</title>" 
                		+ "<genre>Computer</genre>" 
                        + "<price>44.95</price>"
                        + "<publish_date>2000-10-01</publish_date>"
                        + "<description>An in-depth look at creating applications with XML.</description>"
                + "</ann:book>");

        when(tuple.size()).thenReturn(4);
        when(tuple.get(2)).thenReturn(true);
        when(tuple.get(3)).thenReturn(true);
        
        when(tuple.get(1)).thenReturn("book");
        assertEquals(1, xpath.exec(tuple).getAll().size());
        assertEquals(
                "Gambardella, Matthew, XML Developer's Guide, Computer, 44.95, 2000-10-01, An in-depth look at creating applications with XML.",
                xpath.exec(tuple).get(0));

    }

    @Test
    public void testExecTupleWithElementNodeWithComplexNameSpace() throws Exception {

        final XPathAll xpath = new XPathAll();

        final Tuple tuple = mock(Tuple.class);

        when(tuple.get(0)).thenReturn(

                "<cbs:bookstore>"
                        +"<cbs:book>"
                            + "<bsbi:authors>"
                                + "<bsbi:author_1>Gambardella</bsbi:author_1>"
                                + "<bsbi:author_2>Matthew</bsbi:author_2>"
                                + "<bsbi:author_3>Mike</bsbi:author_3>"
                            + "</bsbi:authors>"
                            + "<bsbi:title>23</bsbi:title>"
                            + "<bsbi:genre>semiAutomatic</bsbi:genre>"
                            + "<bsbi:price>enabled</bsbi:price>"
                            + "<bsbi:publish_date>leftToRight</bsbi:publish_date>"
                            + "<bsbi:description>282</bsbi:description>"
                            + "<bsbi:reviews>"
                                + "<review_1>4 stars</review_1>"
                                + "<review_2>3.5 stars</review_2>"
                                + "<review_3>4 stars</review_3>"
                                + "<review_4>4.2 stars</review_4>"
                                + "<review_5>3.5 stars</review_5>"
                            + "</bsbi:reviews>"
                        + "</cbs:book>"
                        + "<cbs:book>"
                        + "<bsbi:authors>"
                            + "<bsbi:author_1>O'Brien</bsbi:author_1>"
                            + "<bsbi:author_2>Tim</bsbi:author_2>"
                        + "</bsbi:authors>"
                        + "<bsbi:title>23</bsbi:title>"
                        + "<bsbi:genre>semiAutomatic</bsbi:genre>"
                        + "<bsbi:price>enabled</bsbi:price>"
                        + "<bsbi:publish_date>leftToRight</bsbi:publish_date>"
                        + "<bsbi:description>282</bsbi:description>"
                        + "<bsbi:reviews>"
                            + "<bsbi:review_1>3.5 stars</bsbi:review_1>"
                            + "<bsbi:review_2>4 stars</bsbi:review_2>"
                            + "<bsbi:review_3>3.5 stars</bsbi:review_3>"
                            + "<bsbi:review_4>4.2 stars</bsbi:review_4>"
                            + "<bsbi:review_5>4 stars</bsbi:review_5>"
                            
                        + "</bsbi:reviews>"
                        + "</cbs:book></cbs:bookstore>");

        when(tuple.size()).thenReturn(4);
        when(tuple.get(2)).thenReturn(true);
        when(tuple.get(3)).thenReturn(true);

        when(tuple.get(1)).thenReturn("bookstore/book/authors");
        assertEquals(2, xpath.exec(tuple).getAll().size());
        assertEquals("Gambardella, Matthew, Mike", xpath.exec(tuple).get(0));
        assertEquals("O'Brien, Tim", xpath.exec(tuple).get(1));

        when(tuple.get(1)).thenReturn("bookstore/book/reviews");
        assertEquals(2, xpath.exec(tuple).getAll().size());
        assertEquals("4 stars, 3.5 stars, 4 stars, 4.2 stars, 3.5 stars", xpath.exec(tuple).get(0));
        assertEquals("3.5 stars, 4 stars, 3.5 stars, 4.2 stars, 4 stars", xpath.exec(tuple).get(1));

    }

    private long timeTheUDF(final Tuple tuple, final XPathAll xpath) throws Exception {

        final long start = System.currentTimeMillis();

        for (int i = 0; i < 50000; i++) {

            tuple.set(0, "<book id=\"bk101"
                    + i
                    + "\">"
                    + // we need to make sure xml changes
                    "<author>Gambardella, Matthew</author>" + "<title>XML Developer's Guide</title>"
                    + "<genre>Computer</genre>" + expandXml() + "<price>44.95</price>"
                    + "<publish_date>2000-10-01</publish_date>"
                    + "<description>An in-depth look at creating applications with XML.</description>" + "</book>");

            // caching is used here. for 2nd and 3rd calls to xpath.exec, the
            // cached javax.xml.xpath.XPath should help
            tuple.set(1, "book/author");
            assertEquals("Gambardella, Matthew", xpath.exec(tuple).get(0));

            tuple.set(1, "book/price");
            assertEquals("44.95", xpath.exec(tuple).get(0));

            tuple.set(1, "book/publish_date");
            assertEquals("2000-10-01", xpath.exec(tuple).get(0));
        }

        return System.currentTimeMillis() - start;
    }

    private String expandXml() {

        final StringBuilder sb = new StringBuilder();

        final int max = RandomUtils.nextInt(100);

        for (int i = 0; i < max; i++) {
            sb.append("<expansion>This is an expansion of the xml to simulate random sized xml" + i + "</expansion>");
        }

        return sb.toString();
    }
}
