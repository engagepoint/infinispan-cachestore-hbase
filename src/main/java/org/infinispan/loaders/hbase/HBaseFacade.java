package org.infinispan.loaders.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.infinispan.loaders.hbase.exception.HBaseException;
import org.infinispan.loaders.hbase.util.HBaseResultScanIterator;
import org.infinispan.loaders.hbase.util.HBaseUtils;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * Adapter class for HBase. Provides a logical abstraction on top of the basic HBase API that makes
 * it easier to use.
 *
 * @author Justin Hayes
 * @since 5.2
 */
public class HBaseFacade {
    private static final Log log = LogFactory.getLog(HBaseFacade.class, Log.class);

    private static final int SCAN_BATCH_SIZE = 100;

    private static Configuration CONFIG;

    /**
     * Create a new HBaseService.
     */
    public HBaseFacade() {
        CONFIG = HBaseConfiguration.create();
    }

    /**
     * Create a new HBaseService.
     */
    public HBaseFacade(Map<String, String> props) {
        this();
        for (Entry<String, String> prop : props.entrySet()) {
            if (prop.getKey() != null && prop.getValue() != null) {
                CONFIG.set(prop.getKey(), prop.getValue());
            }
        }
    }

    /**
     * Creates a new HBase table.
     *
     * @param name           the name of the table
     * @param columnFamilies a list of column family names to use
     * @throws HBaseException
     */
    public void createTable(String name, List<String> columnFamilies) throws HBaseException {
        createTable(name, columnFamilies, HColumnDescriptor.DEFAULT_VERSIONS);
    }

    /**
     * Creates a new HBase table.
     *
     * @param name           the name of the table
     * @param columnFamilies a list of column family names to create
     * @param maxVersions    the max number of versions to maintain for the column families
     * @throws HBaseException
     */
    public void createTable(String name, List<String> columnFamilies, int maxVersions)
            throws HBaseException {
        if (name == null || "".equals(name)) {
            throw new HBaseException("Table name must not be empty.");
        }
        if (tableExists(name)) {
            return;
        }
        try (HBaseAdmin admin = new HBaseAdmin(CONFIG)) {
            HTableDescriptor desc = new HTableDescriptor(name.getBytes());

            // add all column families
            if (columnFamilies != null) {
                for (String cf : columnFamilies) {
                    HColumnDescriptor colFamilyDesc = new HColumnDescriptor(cf.getBytes());
                    colFamilyDesc.setMaxVersions(maxVersions);
                    desc.addFamily(colFamilyDesc);
                }
            }

            int retries = 0;
            do {
                try {
                    admin.createTable(desc);
                    return;
                } catch (PleaseHoldException e) {
                    TimeUnit.SECONDS.sleep(1);
                    retries++;
                }
            } while (retries < 10);
            admin.createTable(desc);
        } catch (Exception ex) {
            throw new HBaseException("Exception occurred when creating HBase table.", ex);
        }
    }

    /**
     * Deletes a HBase table.
     *
     * @param name the name of the table
     * @throws HBaseException
     */
    public void deleteTable(String name) throws HBaseException {
        if (name == null || "".equals(name)) {
            throw new HBaseException("Table name must not be empty.");
        }
        try (HBaseAdmin admin = new HBaseAdmin(CONFIG)) {
            admin.disableTable(name);
            admin.deleteTable(name);
        } catch (Exception ex) {
            throw new HBaseException("Exception occurred when deleting HBase table.", ex);
        }
    }

    /**
     * Checks to see if a table exists.
     *
     * @param name the name of the table
     * @throws HBaseException
     */
    public boolean tableExists(String name) throws HBaseException {
        if (name == null || "".equals(name)) {
            throw new HBaseException("Table name must not be empty.");
        }
        try (HBaseAdmin admin = new HBaseAdmin(CONFIG)) {
            return admin.isTableAvailable(name);
        } catch (Exception ex) {
            throw new HBaseException("Exception occurred when deleting HBase table.", ex);
        }
    }

    /**
     * Adds a row to a HBase table.
     *
     * @param tableName the table to add to
     * @param key       the unique key for the row
     * @param dataMap   the data to add, where the outer map's keys are column family name and values are
     *                  maps that contain the fields and values to add into that column family.
     * @throws HBaseException
     */
    public void addRow(String tableName, String key, Map<String, Map<String, byte[]>> dataMap)
            throws HBaseException {
        if (tableName == null || "".equals(tableName)) {
            throw new HBaseException("Table name must not be empty.");
        }
        if (isEmpty(key)) {
            throw new IllegalArgumentException("key cannot be null or empty.");
        }
        if (isEmpty(dataMap)) {
            throw new IllegalArgumentException("dataMap cannot be null or empty.");
        }

        log.debugf("Writing %s data values to table %s and key %s.", dataMap.size(), tableName, key);

        // write data to HBase, going column family by column family, and field by field
        try (HTable table = new HTable(CONFIG, tableName)) {
            ;
            Put p = new Put(Bytes.toBytes(key));
            for (Entry<String, Map<String, byte[]>> columFamilyEntry : dataMap.entrySet()) {
                String cfName = columFamilyEntry.getKey();
                Map<String, byte[]> cfDataCells = columFamilyEntry.getValue();
                for (Entry<String, byte[]> dataCellEntry : cfDataCells.entrySet()) {
                    p.add(Bytes.toBytes(cfName), Bytes.toBytes(dataCellEntry.getKey()),
                            Bytes.toBytes(ByteBuffer.wrap(dataCellEntry.getValue())));
                }
            }
            table.put(p);
        } catch (IOException ex) {
            throw new HBaseException("Exception happened while " + "writing row to HBase.", ex);
        }
    }

    /**
     * Reads the values in a row from a table.
     *
     * @param tableName      the table to read from
     * @param key            the key for the row
     * @param columnFamilies which column families to return
     * @return a nested map where the outer map's keys are column family name and values are maps
     * that contain the fields and values from that column family.
     * @throws HBaseException
     */
    public Map<String, Map<String, byte[]>> readRow(String tableName, String key,
                                                    List<String> columnFamilies) throws HBaseException {
        if (tableName == null || "".equals(tableName)) {
            throw new HBaseException("Table name must not be empty.");
        }
        if (isEmpty(key)) {
            throw new IllegalArgumentException("key cannot be null or empty.");
        }
        if (isEmpty(columnFamilies)) {
            throw new IllegalArgumentException("columnFamilies cannot be null or empty.");
        }

        log.debugf("Reading row from table %s and key %s.", tableName, key);

        // read data from HBase
        try (HTable table = new HTable(CONFIG, tableName)) {
            ;
            Get g = new Get(Bytes.toBytes(key));
            Result result = table.get(g);
            Map<String, Map<String, byte[]>> resultMap = new HashMap<String, Map<String, byte[]>>(
                    columnFamilies.size());

            // bail if we didn't get any results
            if (result.isEmpty()) {
                return resultMap;
            }

            // extract the fields and values from all column families
            for (String cfName : columnFamilies) {
                Map<String, byte[]> cfDataMap = new HashMap<>();

                Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(cfName));
                for (Entry<byte[], byte[]> familyMapEntry : familyMap.entrySet()) {
                    cfDataMap.put(new String(familyMapEntry.getKey()), familyMapEntry.getValue());
                }

                resultMap.put(cfName, cfDataMap);
            }

            return resultMap;
        } catch (IOException ex) {
            throw new HBaseException("Exception happened while reading row from HBase.", ex);
        }
    }

    /**
     * Reads the values from multiple rows from a table, using a key prefix and a timestamp. For
     * example, if rows were added with keys: key1 key2 key3 key4 Then readRows("myTable", "key", 3,
     * ...) would return the data for rows with key1, key1, and key3.
     *
     * @param tableName    the table to read from
     * @param keyPrefix    the key prefix to use for the query
     * @param ts           timestamp before which rows should be returned
     * @param columnFamily which column family to return
     * @param qualifier    which qualifier (ie field) to return
     * @return a nested map where the outermost map's keys are row keys and values are map whose keys
     * are column family name and values are maps that contain the fields and values from
     * that column family for that row.
     * @throws HBaseException
     */
    public HBaseResultScanIterator readRows(String tableName,
                                            String keyPrefix, long ts, String columnFamily, String qualifier) throws HBaseException {
        if (tableName == null || "".equals(tableName)) {
            throw new HBaseException("Table name must not be empty.");
        }
        if (isEmpty(keyPrefix)) {
            throw new IllegalArgumentException("keyPrefix cannot be null or empty.");
        }
        if (isEmpty(columnFamily)) {
            throw new IllegalArgumentException("columnFamily cannot be null or empty.");
        }

        log.debugf("Reading rows from table %s with key prefix %s and ts %s", tableName, keyPrefix,
                ts);


        Scan scan = new Scan();
        scan.setMaxVersions(Integer.MAX_VALUE);
        scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        scan.setStartRow(Bytes.toBytes(keyPrefix));
        scan.setStopRow(Bytes.toBytes(keyPrefix + ts));

        try {
            return new HBaseResultScanIterator(
                    new HTable(CONFIG, tableName),
                    scan,
                    Integer.MAX_VALUE,
                    columnFamily,
                    qualifier);
        } catch (IOException ex) {
            throw new HBaseException(
                    "Exception happened while " + "scanning table " + tableName + ".", ex);
        }
    }

    /**
     * Removes a row from a table.
     *
     * @param tableName the table to remove from
     * @param key       the key for the row to remove
     * @return true if the row existed and was deleted; false if it didn't exist.
     * @throws HBaseException
     */
    public boolean removeRow(String tableName, String key) throws HBaseException {
        if (tableName == null || "".equals(tableName)) {
            throw new HBaseException("Table name must not be empty.");
        }
        if (isEmpty(key)) {
            throw new IllegalArgumentException("key cannot be null or empty.");
        }

        log.debugf("Removing row from table %s with key %s.", tableName, key);

        // remove data from HBase
        try (HTable table = new HTable(CONFIG, tableName)) {
            // check to see if it exists first
            Get get = new Get(Bytes.toBytes(key));
            boolean exists = table.exists(get);

            if (exists) {
                Delete d = new Delete(Bytes.toBytes(key));
                table.delete(d);
            }

            return exists;
        } catch (IOException ex) {
            throw new HBaseException("Exception happened while " + "deleting row from HBase.", ex);
        }
    }

    /**
     * Removes rows from a table.
     *
     * @param tableName the table to remove from
     * @param keys      a list of keys for the row to remove
     * @throws HBaseException
     */
    public void removeRows(String tableName, Set<Object> keys) throws HBaseException {
        if (tableName == null || "".equals(tableName)) {
            throw new HBaseException("Table name must not be empty.");
        }
        if (isEmpty(keys)) {
            throw new IllegalArgumentException("keys cannot be null or empty.");
        }

        log.debugf("Removing rows from table %s.", tableName);

        // remove data from HBase
        try (HTable table = new HTable(CONFIG, tableName)) {
            List<Delete> deletes = new ArrayList<Delete>(keys.size());
            for (Object key : keys) {
                deletes.add(new Delete(Bytes.toBytes((String) key)));
            }

            table.delete(deletes);
        } catch (IOException ex) {
            throw new HBaseException("Exception happened while " + "deleting rows from HBase.", ex);
        }
    }

    /**
     * Scans an entire table, returning the values from the given column family and field for each
     * row.
     * <p/>
     * TODO - maybe update to accept multiple column families and fields and return a Map<String,
     * Map<String, Map<String, byte[]>>>
     *
     * @param tableName    the table to scan
     * @param columnFamily the column family of the field to return
     * @param qualifier    the field to return
     * @return map mapping row keys to values for all rows
     * @throws HBaseException
     */
    public HBaseResultScanIterator scan(String tableName, String columnFamily, String qualifier)
            throws HBaseException {
        return scan(tableName, Integer.MAX_VALUE, columnFamily, qualifier);
    }

    /**
     * Scans an entire table, returning the values from the given column family and field for each
     * row.
     * <p/>
     * TODO - maybe update to accept multiple column families and fields and return a Map<String,
     * Map<String, Map<String, byte[]>>>
     *
     * @param tableName    the table to scan
     * @param numEntries   the max number of entries to return; if < 0, defaults to Integer.MAX_VALUE
     * @param columnFamily the column family of the field to return
     * @param qualifier    the field to return
     * @return map mapping row keys to values for all rows
     * @throws HBaseException
     */
    public HBaseResultScanIterator scan(String tableName, int numEntries, String columnFamily,
                                        String qualifier) throws HBaseException {
        if (isEmpty(tableName)) {
            throw new IllegalArgumentException("tableName cannot be null or empty.");
        }
        if (isEmpty(columnFamily)) {
            throw new IllegalArgumentException("colFamily cannot be null or empty.");
        }
        if (isEmpty(qualifier)) {
            throw new IllegalArgumentException("field cannot be null or empty.");
        }

        // default to maximum number of entries if not specified
        if (numEntries < 0) {
            numEntries = Integer.MAX_VALUE;
        }

        log.debugf("Scanning table %s.", tableName);

        // read data from HBase
        try {
            return new HBaseResultScanIterator(new HTable(CONFIG, tableName), numEntries, columnFamily, qualifier);
        } catch (IOException ex) {
            throw new HBaseException(
                    "Exception happened while " + "scanning table " + tableName + ".", ex);
        }
    }

    /**
     * Returns a set of all unique keys for a given table.
     *
     * @param tableName the table to return the keys for
     * @return
     * @throws HBaseException
     */
    public Set<Object> scanForKeys(String tableName) throws HBaseException {
        if (isEmpty(tableName)) {
            throw new IllegalArgumentException("tableName cannot be null or empty.");
        }

        log.debugf("Scanning table %s for keys.", tableName);

        // scan the entire table, extracting the key from each row
        try (HTable table = new HTable(CONFIG, tableName);
             ResultScanner scanner = table.getScanner(new Scan())) {
            Set<Object> results = new HashSet<>();

            // batch the scan to improve performance
            Result[] resultBatch = scanner.next(SCAN_BATCH_SIZE);
            while (resultBatch != null && resultBatch.length > 0) {
                for (Result aResultBatch : resultBatch) {
                    String key = HBaseUtils.getKeyFromResult(aResultBatch);
                    results.add(key);
                }

                // get the next batch
                resultBatch = scanner.next(SCAN_BATCH_SIZE);
            }
            return results;
        } catch (IOException ex) {
            throw new HBaseException(
                    "Exception happened while " + "scanning table " + tableName + ".", ex);
        }
    }

    /**
     * Returns true if the argument is null or is "empty", which is determined based on the type of
     * the argument.
     *
     * @param o
     * @return
     */
    private boolean isEmpty(Object o) {
        if (o == null) {
            return true;
        } else if (o instanceof String && "".equals(o)) {
            return true;
        } else if (o instanceof List<?> && ((List<?>) o).size() < 1) {
            return true;
        } else if (o instanceof Map<?, ?> && ((Map<?, ?>) o).size() < 1) {
            return true;
        } else if (o instanceof byte[] && ((byte[]) o).length < 1) {
            return true;
        }

        return false;
    }


}
