package org.infinispan.loaders.hbase.util;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class HBaseResultScanIterator implements Iterator<Map.Entry>, AutoCloseable {

    private static final Log log = LogFactory.getLog(HBaseResultScanIterator.class, Log.class);
    private static final int SCAN_BATCH_SIZE = 100;

    private final HTable table;
    private final ResultScanner scanner;
    private final TreeMap<String, byte[]> buffer;
    private final int numEntries;
    private final String columnFamily;
    private final String qualifier;
    private final int scanBatchSize;
    private int ct = 0;

    public HBaseResultScanIterator(HTable table, int numEntries, String columnFamily, String qualifier) {
        this(table, numEntries, columnFamily, qualifier, SCAN_BATCH_SIZE);
    }

    public HBaseResultScanIterator(HTable table, Scan scan, int numEntries, String columnFamily, String qualifier) {
        this(table, getScanner(table, scan), numEntries, columnFamily, qualifier, SCAN_BATCH_SIZE);
    }

    public HBaseResultScanIterator(HTable table, int numEntries, String columnFamily, String qualifier, int scanBatchSize) {
        this(table, getScanner(table, columnFamily, qualifier), numEntries, columnFamily, qualifier, scanBatchSize);
    }

    private HBaseResultScanIterator(HTable table, ResultScanner scanner, int numEntries, String columnFamily, String qualifier, int scanBatchSize) {
        this.table = table;
        this.scanner = scanner;
        this.numEntries = numEntries;
        this.columnFamily = columnFamily;
        this.qualifier = qualifier;
        this.scanBatchSize = scanBatchSize;
        buffer = new TreeMap<>();
    }

    @Override
    public boolean hasNext() {
        while (buffer.isEmpty() && ct < numEntries) {
            int batchSize = Math.min(scanBatchSize, numEntries - ct);
            Result[] batch = nextResultFromScanner(batchSize);
            if (batch.length <= 0) {
                break;
            } else {
                for (Result curr : batch) {
                    // extract the data for this row
                    String key = HBaseUtils.getKeyFromResult(curr);

                    byte[] valueArr = null;
                    if (curr.containsColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier))) {
                        valueArr = curr
                                .getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                        log.tracef("Value=%s", Bytes.toString(valueArr));
                    }

                    log.tracef("Added %s->%s", key, Bytes.toString(valueArr));
                    buffer.put(key, valueArr);
                }
                ct += batch.length;
            }
        }
        return !buffer.isEmpty();
    }

    @Override
    public Map.Entry<String, byte[]> next() {
        return buffer.pollFirstEntry();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        try {
            table.close();
            if (scanner != null) {
                scanner.close();
            }
        } catch (Exception ex) {
            // do nothing
        }
    }

    private Result[] nextResultFromScanner(int batchSize) {
        try {
            return scanner.next(batchSize);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


    private static ResultScanner getScanner(HTable table, Scan scan) {
        try {
            return table.getScanner(scan);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static ResultScanner getScanner(HTable table, String columnFamily, String qualifier) {
        try {
            return table.getScanner(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
