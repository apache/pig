/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.hbase;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.impl.util.UDFContext;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Class to test how filters are used by HBaseStorage, since mis-use can be costly in terms of
 * perfomance.
 */
public class TestHBaseStorageFiltering {

    private Filter getHBaseStorageFilter(String firstArg)
            throws NoSuchFieldException, ParseException, IOException, IllegalAccessException {
        return initHBaseStorageFilter(firstArg, "");
    }

    // Helper to initialize HBaseStorage and return its Filter
    private Filter initHBaseStorageFilter(String firstArg, String secondArg)
            throws NoSuchFieldException, ParseException, IOException, IllegalAccessException {
        UDFContext.getUDFContext().setClientSystemProps(new Properties());

        Field scanField = HBaseStorage.class.getDeclaredField("scan");
        scanField.setAccessible(true);

        HBaseStorage storage = new HBaseStorage(firstArg, secondArg);
        Scan scan = (Scan)scanField.get(storage);
        return scan.getFilter();
    }

    // Helper to initialize HBaseStorage and return its List of ColumnInfo objects
    @SuppressWarnings("unchecked")
    private List<HBaseStorage.ColumnInfo> getHBaseColumnInfo(String firstArg, String secondArg)
            throws NoSuchFieldException, ParseException, IOException, IllegalAccessException {
        UDFContext.getUDFContext().setClientSystemProps(new Properties());

        Field columnInfoField = HBaseStorage.class.getDeclaredField("columnInfo_");
        columnInfoField.setAccessible(true);

        HBaseStorage storage = new HBaseStorage(firstArg, secondArg);
        List<HBaseStorage.ColumnInfo> columnInfoList =
                (List<HBaseStorage.ColumnInfo>)columnInfoField.get(storage);
        return columnInfoList;
    }

    @Test
    public void testDescriptors() throws Exception {
        Filter filter = getHBaseStorageFilter("cf1:a cf1:b");
        assertNull(filter);
    }

    @Test
    public void testFamily() throws Exception {
        Filter filter = getHBaseStorageFilter("cf1:");
        assertNull(filter);
    }

    @Test
    public void testFamily2() throws Exception {
        Filter filter = getHBaseStorageFilter("cf1:*");
        assertNull(filter);
    }

    @Test
    public void testPrefix() throws Exception {
        Filter filter = getHBaseStorageFilter("cf1:foo*");
        List<Filter> childFilters = assertFilterList(filter, FilterList.Operator.MUST_PASS_ALL, 1);
        childFilters = assertFilterList(childFilters.get(0), FilterList.Operator.MUST_PASS_ONE, 1);
        childFilters = assertFilterList(childFilters.get(0), FilterList.Operator.MUST_PASS_ALL, 2);
        assertFamilyFilter(childFilters.get(0), CompareFilter.CompareOp.EQUAL, "cf1");
        childFilters = assertFilterList(childFilters.get(1), FilterList.Operator.MUST_PASS_ONE, 1);
        assertPrefixFilter(childFilters.get(0), "foo");
    }

    @Test
    public void testDescriptorsAndFamily1() throws Exception {
        Filter filter = getHBaseStorageFilter("cf1:a cf1:b cf2:");
        assertNull(filter);
    }

    @Test
    public void testDescriptorsAndFamily2() throws Exception {
        Filter filter = getHBaseStorageFilter("cf1:a cf1:b cf2:*");
        assertNull(filter);
    }

    @Test
    public void testDescriptorsAndPrefix() throws Exception {
        Filter filter = getHBaseStorageFilter("cf1:a cf1:b cf2:foo*");
        List<Filter> childFilters = assertFilterList(filter, FilterList.Operator.MUST_PASS_ALL, 1);
        List<Filter> groupFilters = assertFilterList(childFilters.get(0), FilterList.Operator.MUST_PASS_ONE, 2);

        List<Filter> firstFilters = assertFilterList(groupFilters.get(0), FilterList.Operator.MUST_PASS_ALL, 2);
        FamilyFilter firstFamilyFilter = assertFamilyFilter(firstFilters.get(0), CompareFilter.CompareOp.EQUAL);

        List<Filter> secondFilters = assertFilterList(groupFilters.get(1), FilterList.Operator.MUST_PASS_ALL, 2);
        FamilyFilter secondFamilyFilter = assertFamilyFilter(secondFilters.get(0), CompareFilter.CompareOp.EQUAL);

        // one of the above will be the cf1 filters, one will be the cf2. Order is unknown
        Filter cf1ColumnList;
        Filter cf2ColumnList;
        if (Bytes.toString(firstFamilyFilter.getComparator().getValue()).equals("cf1")) {
            assertEquals("cf2", Bytes.toString(secondFamilyFilter.getComparator().getValue()));
            cf1ColumnList = firstFilters.get(1);
            cf2ColumnList = secondFilters.get(1);
        }
        else {
            assertEquals("cf1", Bytes.toString(secondFamilyFilter.getComparator().getValue()));
            assertEquals("cf2", Bytes.toString(firstFamilyFilter.getComparator().getValue()));
            cf1ColumnList = secondFilters.get(1);
            cf2ColumnList = firstFilters.get(1);
        }

        List<Filter> c1ColumnFilters = assertFilterList(cf1ColumnList, FilterList.Operator.MUST_PASS_ONE, 2);
        assertQualifierFilter(c1ColumnFilters.get(0), CompareFilter.CompareOp.EQUAL, "a");
        assertQualifierFilter(c1ColumnFilters.get(1), CompareFilter.CompareOp.EQUAL, "b");

        List<Filter> c2ColumnFilters = assertFilterList(cf2ColumnList, FilterList.Operator.MUST_PASS_ONE, 1);
        assertPrefixFilter(c2ColumnFilters.get(0), "foo");
    }

    @Test
    public void testColumnGroups() throws Exception {
        List<HBaseStorage.ColumnInfo> columnInfoList = getHBaseColumnInfo("cf1:a cf1:b cf2:foo*", "");
        Map<String, List<HBaseStorage.ColumnInfo>> groupMap = HBaseStorage.groupByFamily(columnInfoList);
        assertEquals(2, groupMap.size());

        List<HBaseStorage.ColumnInfo> cf1List = groupMap.get("cf1");
        assertNotNull(cf1List);
        assertEquals(2, cf1List.size());
        assertColumnInfo(cf1List.get(0), "cf1", "a", null);
        assertColumnInfo(cf1List.get(1), "cf1", "b", null);

        List<HBaseStorage.ColumnInfo> cf2List = groupMap.get("cf2");
        assertNotNull(cf2List);
        assertEquals(1, cf2List.size());
        assertColumnInfo(cf2List.get(0), "cf2", null, "foo");
    }

    @Test
    public void testIncrementStrings() {
        doIncrementTest("100", "101");
        doIncrementTest("0001", "0002");
        doIncrementTest("aaaccccc", "aaaccccd");
    }

    @Test
    public void testIncrementBytes() {
        doIncrementTest(Bytes.toBytes(0x03), Bytes.toBytes(0x04));
        doIncrementTest(new byte[] {0, 1, 0}, new byte[] {0, 1, 1});
        doIncrementTest(new byte[] {127}, new byte[] {-128});
        doIncrementTest(new byte[] {-1}, new byte[] {-1, 0});
        doIncrementTest(new byte[] {-1, -1}, new byte[] {-1, -1, 0});
        doIncrementTest(new byte[] {0, -1, -1}, new byte[] {1, 0, 0});
        doIncrementTest(Bytes.toBytes(0xFFFFFFFF), new byte[] {-1, -1, -1, -1, 0});
        doIncrementTest(Bytes.toBytes(Long.MAX_VALUE), new byte[] {-128, 0, 0, 0, 0, 0, 0, 0});
    }

    private void doIncrementTest(String initial, String expected) {
        byte[] initialBytes = Bytes.toBytes(initial);
        byte[] expectedBytes = Bytes.toBytes(expected);
        byte[] incrementedBytes = HBaseStorage.increment(initialBytes);
        assertTrue(String.format("Expected bytes %s (%s) did not equal found bytes %s (%s)",
                HBaseStorage.toString(expectedBytes), expected, HBaseStorage.toString(incrementedBytes), Bytes.toString(incrementedBytes)),
                Bytes.compareTo(expectedBytes, incrementedBytes) == 0);
        assertEquals(expected, Bytes.toString(incrementedBytes));
        assertTrue("Initial value of " + initial + " should be < " + Bytes.toString(incrementedBytes),
                Bytes.compareTo(initialBytes, incrementedBytes) < 0);
    }

    private void doIncrementTest(byte[] initial, byte[] expected) {
        byte[] incrementedBytes = HBaseStorage.increment(initial);
        if (expected == null) {
            assertNull("Expected null bytes but found " + HBaseStorage.toString(incrementedBytes), incrementedBytes);
        } else {
            assertTrue(String.format("Expected bytes %s did not equal found bytes %s",
                    HBaseStorage.toString(expected), HBaseStorage.toString(incrementedBytes)),
                    Bytes.compareTo(expected, incrementedBytes) == 0);
        }
        assertTrue("Initial value of should be < incremented value",
                Bytes.compareTo(initial, incrementedBytes) < 0);
    }

    private void assertColumnInfo(HBaseStorage.ColumnInfo columnInfo,
                                  String columnFamily, String columnName, String prefix) {
        assertEquals(columnFamily, Bytes.toString(columnInfo.getColumnFamily()));
        assertEquals(columnName, Bytes.toString(columnInfo.getColumnName()));
        assertEquals(prefix, Bytes.toString(columnInfo.getColumnPrefix()));
    }

    private List<Filter> assertFilterList(Filter filter, FilterList.Operator operator, int size) {
        assertTrue("Filter is not a FilterList: " + filter.getClass().getSimpleName(),
                filter instanceof FilterList);
        FilterList filterList = (FilterList)filter;
        assertEquals("Unexpected operator", operator, filterList.getOperator());
        assertEquals("Unexpected filter list size", size, filterList.getFilters().size());
        return filterList.getFilters();
    }

    private FamilyFilter assertFamilyFilter(Filter filter, CompareFilter.CompareOp compareOp, String value) {
        FamilyFilter familyFilter = assertFamilyFilter(filter, compareOp);
        assertEquals("Unexpected value", value, Bytes.toString(familyFilter.getComparator().getValue()));
        return familyFilter;
    }

    private FamilyFilter assertFamilyFilter(Filter filter, CompareFilter.CompareOp compareOp) {
        assertTrue("Filter is not a FamilyFilter: " + filter.getClass().getSimpleName(),
                filter instanceof FamilyFilter);
        FamilyFilter familyFilter = (FamilyFilter)filter;
        assertEquals("Unexpected compareOp", compareOp, familyFilter.getOperator());
        return familyFilter;
    }

    private void assertPrefixFilter(Filter filter, String prefix) {
        assertTrue("Filter is not a ColumnPrefixFilter: " + filter.getClass().getSimpleName(),
                filter instanceof ColumnPrefixFilter);
        ColumnPrefixFilter familyFilter = (ColumnPrefixFilter)filter;
        assertEquals("Unexpected prefix", prefix, Bytes.toString(familyFilter.getPrefix()));
    }

    private void assertQualifierFilter(Filter filter, CompareFilter.CompareOp compareOp, String value) {
        assertTrue("Filter is not a QualifierFilter: " + filter.getClass().getSimpleName(),
                filter instanceof QualifierFilter);
        QualifierFilter qualifierFilter = (QualifierFilter)filter;
        assertEquals("Unexpected compareOp", compareOp, qualifierFilter.getOperator());
        assertEquals("Unexpected value", value, Bytes.toString(qualifierFilter.getComparator().getValue()));
    }

    private void printFilters(List<Filter> filters) {
        for (Filter child : filters) {
            printFilters(child);
        }
    }

    private void printFilters(Filter filter) {
        if (filter != null) {
            System.out.println(filter.toString());
            if (filter instanceof FilterList) {
                printFilters(((FilterList)filter).getFilters());
            }
        }
    }
}
