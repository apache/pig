/* Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;

public class HBaseTableInputFormat extends TableInputFormat {
    private static final Log LOG = LogFactory.getLog(HBaseTableInputFormat.class);

    protected final byte[] gt_;
    protected final byte[] gte_;
    protected final byte[] lt_;
    protected final byte[] lte_;

    public HBaseTableInputFormat() {
        this(-1, null, null, null, null);
    }

    protected HBaseTableInputFormat(long limit, byte[] gt, byte[] gte, byte[] lt, byte[] lte) {
        super();
        setTableRecordReader(new HBaseTableRecordReader(limit));
        gt_ = gt;
        gte_ = gte;
        lt_ = lt;
        lte_ = lte;
    }

    public static class HBaseTableIFBuilder {
        protected byte[] gt_;
        protected byte[] gte_;
        protected byte[] lt_;
        protected byte[] lte_;
        protected long limit_;
        protected Configuration conf_;

        public HBaseTableIFBuilder withGt(byte[] gt) { gt_ = gt; return this; }
        public HBaseTableIFBuilder withGte(byte[] gte) { gte_ = gte; return this; }
        public HBaseTableIFBuilder withLt(byte[] lt) { lt_ = lt; return this; }
        public HBaseTableIFBuilder withLte(byte[] lte) { lte_ = lte; return this; }
        public HBaseTableIFBuilder withLimit(long limit) { limit_ = limit; return this; }
        public HBaseTableIFBuilder withConf(Configuration conf) { conf_ = conf; return this; }

        public HBaseTableInputFormat build() {
            HBaseTableInputFormat inputFormat = new HBaseTableInputFormat(limit_, gt_, gte_, lt_, lte_);
            if (conf_ != null) inputFormat.setConf(conf_);
            return inputFormat;
        }

    }

    @Override
    public List<InputSplit> getSplits(org.apache.hadoop.mapreduce.JobContext context)
    throws IOException {
        List<InputSplit> splits = super.getSplits(context);
        ListIterator<InputSplit> splitIter = splits.listIterator();
        while (splitIter.hasNext()) {
            TableSplit split = (TableSplit) splitIter.next();
            byte[] startKey = split.getStartRow();
            byte[] endKey = split.getEndRow();
            // Skip if the region doesn't satisfy configured options.
            if ((skipRegion(CompareOp.LESS, startKey, lt_)) ||
                    (skipRegion(CompareOp.GREATER, endKey, gt_)) ||
                    (skipRegion(CompareOp.GREATER, endKey, gte_)) ||
                    (skipRegion(CompareOp.LESS_OR_EQUAL, startKey, lte_)) )  {
                splitIter.remove();
            }
        }
        return splits;
    }

    private boolean skipRegion(CompareOp op, byte[] key, byte[] option ) {

        if (key.length == 0 || option == null) 
            return false;

        BinaryComparator comp = new BinaryComparator(option);
        RowFilter rowFilter = new RowFilter(op, comp);
        return rowFilter.filterRowKey(key, 0, key.length);
    }

    protected class HBaseTableRecordReader extends TableRecordReader {

        private long recordsSeen = 0;
        private final long limit_;
        private byte[] startRow_;
        private byte[] endRow_;
        private transient byte[] currRow_;

        private BigInteger bigStart_;
        private BigInteger bigEnd_;
        private BigDecimal bigRange_;
        private transient float progressSoFar_ = 0;

        public HBaseTableRecordReader(long limit) {
            limit_ = limit;
        }

        @Override
        public void setScan(Scan scan) {
            super.setScan(scan);

            startRow_ = scan.getStartRow();
            endRow_ = scan.getStopRow();
            byte[] startPadded;
            byte[] endPadded;
            if (startRow_.length < endRow_.length) {
                startPadded = Bytes.padTail(startRow_, endRow_.length - startRow_.length);
                endPadded = endRow_;
            } else if (endRow_.length < startRow_.length) {
                startPadded = startRow_;
                endPadded = Bytes.padTail(endRow_, startRow_.length - endRow_.length);
            } else {
                startPadded = startRow_;
                endPadded = endRow_;
            }
            currRow_ = startRow_;
            byte [] prependHeader = {1, 0};
            bigStart_ = new BigInteger(Bytes.add(prependHeader, startPadded));
            bigEnd_ = new BigInteger(Bytes.add(prependHeader, endPadded));
            bigRange_ = new BigDecimal(bigEnd_.subtract(bigStart_));
            LOG.info("setScan with ranges: " + bigStart_ + " - " + bigEnd_ + " ( " + bigRange_ + ")");
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (limit_ > 0 && ++recordsSeen > limit_) {
                return false;
            }
            boolean hasMore = super.nextKeyValue();
            if (hasMore) {
                currRow_ = getCurrentKey().get();
            }
            return hasMore;

        }

        @Override
        public float getProgress() {
            if (currRow_ == null || currRow_.length == 0 || endRow_.length == 0 || endRow_ == HConstants.LAST_ROW) {
                return 0;
            }
            byte[] lastPadded = currRow_;
            if (currRow_.length < endRow_.length) {
                lastPadded = Bytes.padTail(currRow_, endRow_.length - currRow_.length);
            }
            if (currRow_.length < startRow_.length) {
                lastPadded = Bytes.padTail(currRow_, startRow_.length - currRow_.length);
            }
            byte [] prependHeader = {1, 0};
            BigInteger bigLastRow = new BigInteger(Bytes.add(prependHeader, lastPadded));
            if (bigLastRow.compareTo(bigEnd_) > 0) {
                return progressSoFar_;
            }
            BigDecimal processed = new BigDecimal(bigLastRow.subtract(bigStart_));
            try {
                BigDecimal progress = processed.setScale(3).divide(bigRange_, BigDecimal.ROUND_HALF_DOWN);
                progressSoFar_ = progress.floatValue();
                return progressSoFar_;
            } catch (java.lang.ArithmeticException e) {
                return 0;
            }            
        }

    }
}
