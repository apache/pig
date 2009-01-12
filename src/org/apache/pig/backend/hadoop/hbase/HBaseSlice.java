/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.backend.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.pig.Slice;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * HBase Slice to load a portion of range of a table. The key range will be
 * [start, end) Modeled from org.apache.hadoop.hbase.mapred.TableSplit.
 */
public class HBaseSlice implements Slice {

    /** A Generated Serial Version UID **/
    private static final long serialVersionUID = 9035916017187148965L;
    private static final Log LOG = LogFactory.getLog(HBaseSlice.class);

    // assigned during construction
    /** Table Name **/
    private byte[] m_tableName;
    /** Table Start Row **/
    private byte[] m_startRow;
    /** Table End Row **/
    private byte[] m_endRow;
    /** Table Region Location **/
    private String m_regionLocation;
    /** Input Columns **/
    private byte[][] m_inputColumns;

    // created as part of init
    /** The connection to the table in Hbase **/
    private transient HTable m_table;
    /** The scanner over the table **/
    private transient Scanner m_scanner;

    private transient ArrayList<Object> mProtoTuple;

    /**
     * Record the last processed row, so that we can restart the scanner when an
     * exception happened during scanning a table
     */
    private transient byte[] m_lastRow;

    /**
     * Constructor
     * 
     * @param tableName
     *            table name
     * @param startRow
     *            start now, inclusive
     * @param endRow
     *            end row, exclusive
     * @param inputColumns
     *            input columns
     * @param location
     *            region location
     */
    public HBaseSlice(byte[] tableName, byte[] startRow, byte[] endRow,
            byte[][] inputColumns, final String location) {
        this.m_tableName = tableName;
        this.m_startRow = startRow;
        this.m_endRow = endRow;
        this.m_inputColumns = inputColumns;
        this.m_regionLocation = location;
    }

    /** @return table name */
    public byte[] getTableName() {
        return this.m_tableName;
    }

    /** @return starting row key */
    public byte[] getStartRow() {
        return this.m_startRow;
    }

    /** @return end row key */
    public byte[] getEndRow() {
        return this.m_endRow;
    }

    /** @return input columns */
    public byte[][] getInputColumns() {
        return this.m_inputColumns;
    }

    /** @return the region's hostname */
    public String getRegionLocation() {
        return this.m_regionLocation;
    }

    @Override
    public long getStart() {
        // Not clear how to obtain this in a table...
        return 0;
    }

    @Override
    public long getLength() {
        // Not clear how to obtain this in a table...
        // it seems to be used only for sorting splits
        return 0;
    }

    @Override
    public String[] getLocations() {
        return new String[] { m_regionLocation };
    }

    @Override
    public long getPos() throws IOException {
        // This should be the ordinal tuple in the range;
        // not clear how to calculate...
        return 0;
    }

    @Override
    public float getProgress() throws IOException {
        // Depends on the total number of tuples and getPos
        return 0;
    }

    @Override
    public void init(DataStorage store) throws IOException {
        LOG.info("Init Hbase Slice " + this);
        HBaseConfiguration conf = new HBaseConfiguration();
        // connect to the given table
        m_table = new HTable(conf, m_tableName);
        // init the scanner
        init_scanner();
    }

    /**
     * Init the table scanner
     * 
     * @throws IOException
     */
    private void init_scanner() throws IOException {
        restart(m_startRow);
        m_lastRow = m_startRow;
    }

    /**
     * Restart scanning from survivable exceptions by creating a new scanner.
     * 
     * @param startRow
     *            the start row
     * @throws IOException
     */
    private void restart(byte[] startRow) throws IOException {
        if ((m_endRow != null) && (m_endRow.length > 0)) {
            this.m_scanner = this.m_table.getScanner(m_inputColumns, startRow,
                    m_endRow);
        } else {
            this.m_scanner = this.m_table.getScanner(m_inputColumns, startRow);
        }
    }

    @Override
    public boolean next(Tuple value) throws IOException {
        RowResult result;
        try {
            result = this.m_scanner.next();
        } catch (UnknownScannerException e) {
            LOG.debug("recovered from " + StringUtils.stringifyException(e));
            restart(m_lastRow);
            if (m_lastRow != m_startRow) {
                this.m_scanner.next(); // skip presumed already mapped row
            }
            result = this.m_scanner.next();
        }
        boolean hasMore = result != null && result.size() > 0;
        if (hasMore) {
            m_lastRow = result.getRow();
            convertResultToTuple(result, value);
        }
        return hasMore;
    }

    /**
     * Converte a row result to a tuple
     * 
     * @param result
     *            row result
     * @param tuple
     *            tuple
     */
    private void convertResultToTuple(RowResult result, Tuple tuple) {
        if (mProtoTuple == null)
            mProtoTuple = new ArrayList<Object>();

        Cell cell = null;
        byte[] value = null;
        for (byte[] column : m_inputColumns) {
            cell = result.get(column);
            if (cell == null || (value = cell.getValue()) == null) {
                mProtoTuple.add(null);
            } else {
                mProtoTuple.add(new DataByteArray(value));
            }
        }

        Tuple newT = TupleFactory.getInstance().newTuple(mProtoTuple);
        mProtoTuple.clear();
        tuple.reference(newT);
    }

    @Override
    public void close() throws IOException {
        if (m_scanner != null) {
            m_scanner.close();
            m_scanner = null;
        }
    }

    @Override
    public String toString() {
        return m_regionLocation + ":" + Bytes.toString(m_startRow) + ","
                + Bytes.toString(m_endRow);
    }

}
