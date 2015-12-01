package org.infinispan.loaders.hbase;

import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.filter.KeyFilter;
import org.infinispan.loaders.hbase.configuration.HBaseCacheStoreConfiguration;
import org.infinispan.loaders.hbase.exception.HBaseException;
import org.infinispan.loaders.hbase.util.HBaseResultScanIterator;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.UnsupportedKeyTypeException;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import static org.infinispan.persistence.PersistenceUtil.getExpiryTime;

@ConfiguredBy(HBaseCacheStoreConfiguration.class)
public class HBaseCacheStore implements AdvancedLoadWriteStore {
    private static final Log log = LogFactory.getLog(HBaseCacheStore.class, Log.class);

    private HBaseCacheStoreConfiguration configuration;
    private TwoWayKey2StringMapper key2StringMapper;
    private InitializationContext ctx;
    private GlobalConfiguration globalConfiguration;
    private HBaseFacade hbf;
    private String entryKeyPrefix;
    private String expirationKeyPrefix;

    @Override
    public void init(InitializationContext ctx) {
        log.debug("Hbase cache store initialising");
        this.configuration = ctx.getConfiguration();
        this.ctx = ctx;
        globalConfiguration = ctx.getCache().getCacheManager().getCacheManagerConfiguration();

        // config for entries
        String cacheName = ctx.getCache().getName();
        entryKeyPrefix = "e_" + (configuration.sharedTable() ? cacheName + "_" : "");
        expirationKeyPrefix = "x_" + (configuration.sharedTable() ? cacheName + "_" : "");
    }

    @Override
    public void start() {
        log.info("Hbase cache store starting");

        hbf = new HBaseFacade(prepareHbaseConfiguration());

        // create the cache store table if necessary
        if (configuration.autoCreateTable()) {
            log.infof("Automatically creating %s and %s tables.", configuration.entryTable(),
                    configuration.expirationTable());
            createTable(configuration.entryTable(), configuration.entryColumnFamily());
            createTable(configuration.expirationTable(), configuration.expirationColumnFamily());
        }

        prepareKey2StringMapper();

        log.info("HBaseCacheStore started");
    }

    @Override
    public void process(
            final KeyFilter filter,
            final CacheLoaderTask task,
            Executor executor,
            final boolean fetchValue,
            final boolean fetchMetadata) {
        final InitializationContext ctx = this.ctx;
        final TaskContext taskContext = new TaskContextImpl();
        log.info("HBaseCacheStore process started");
        ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<Void>(executor);

        Future<Void> future = ecs.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                try (HBaseResultScanIterator values = hbf.scan(
                        configuration.entryTable(),
                        configuration.entryColumnFamily(),
                        configuration.entryValueField())) {
                    while (values.hasNext()) {
                        Entry<String, byte[]> valueEntry = values.next();

                        Object key = unhashKey(entryKeyPrefix, valueEntry.getKey());
                        if (taskContext.isStopped()) {
                            break;
                        }
                        if (filter != null && !filter.accept(key)) {
                            continue;
                        }
                        MarshalledEntry entry;
                        if (fetchValue || fetchMetadata) {
                            byte[] value = valueEntry.getValue();

                            if (value == null) {
                                return null;
                            }
                            entry = unmarshall(value);
                        } else {
                            entry = ctx.getMarshalledEntryFactory().newMarshalledEntry(key, (Object) null, null);
                        }
                        task.processEntry(entry, taskContext);
                    }
                }
                return null;
            }
        });
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void stop() {
        log.info("Hbase cache store stopping");
    }

    @Override
    public void write(MarshalledEntry entry) {
        log.debugf("In HBaseCacheStore.store for %s: %s", configuration.entryTable(), entry.getKey());

        Object key = entry.getKey();
        String hashedKey = hashKey(this.entryKeyPrefix, key);

        try {
            writeEntry(entry, hashedKey);

            if (canExpire(entry)) {
                writeExpireEntry(entry, hashedKey);
            }

        } catch (HBaseException ex) {
            log.error("HadoopException storing entry: " + ex.getMessage());
            throw new PersistenceException(ex);
        } catch (Exception ex2) {
            log.error("Exception storing entry: " + ex2.getMessage());
            throw new PersistenceException(ex2);
        }

    }

    /**
     * Removes all entries from the cache. This include removing items from the expiration table.
     */
    @Override
    public void clear() {
        // clear both the entry table and the expiration table
        String[] tableNames = {configuration.entryTable(), configuration.expirationTable()};
        String[] keyPrefixes = {this.entryKeyPrefix, this.expirationKeyPrefix};

        for (int i = 0; i < tableNames.length; i++) {
            // get all keys for this table
            Set<Object> allKeys = loadAllKeysForTable(tableNames[i], null);
            Set<Object> allKeysHashed = new HashSet<Object>(allKeys.size());
            for (Object key : allKeys) {
                allKeysHashed.add(hashKey(keyPrefixes[i], key));
            }

            // remove the rows for those keys
            try {
                hbf.removeRows(tableNames[i], allKeysHashed);
            } catch (HBaseException ex) {
                log.error("Caught HadoopException clearing the " + tableNames[i] + " table: "
                        + ex.getMessage());
                throw new PersistenceException(ex);
            }
        }
    }

    /**
     * Removes an entry from the cache, given its key.
     *
     * @param key the key for the entry to remove.
     */
    @Override
    public boolean delete(Object key) {
        log.debugf("In HBaseCacheStore.remove for key %s", key);

        String hashedKey = hashKey(this.entryKeyPrefix, key);
        try {
            return hbf.removeRow(configuration.entryTable(), hashedKey);
        } catch (HBaseException ex) {
            log.error("HadoopException removing an object from the cache: " + ex.getMessage(), ex);
            throw new PersistenceException("HadoopException removing an object from the cache: "
                    + ex.getMessage(), ex);
        }
    }

    /**
     * Loads an entry from the cache, given its key.
     *
     * @param key the key for the entry to load.
     */
    @Override
    public MarshalledEntry load(Object key) {
        log.debugf("In HBaseCacheStore.load for key %s", key);

        String hashedKey = hashKey(this.entryKeyPrefix, key);
        List<String> colFamilies = Collections.singletonList(configuration.entryColumnFamily());

        try {
            Map<String, Map<String, byte[]>> resultMap = hbf.readRow(configuration.entryTable(), hashedKey,
                    colFamilies);
            if (resultMap.isEmpty()) {
                log.debugf("Key %s not found.", hashedKey);
                return null;
            }

            byte[] value = resultMap.get(configuration.entryColumnFamily()).get(configuration.entryValueField());
            MarshalledEntry marshalledEntry = unmarshall(value);

            if (isExpired(marshalledEntry, ctx.getTimeService().wallClockTime())) {
                delete(key);
                return null;
            }

            return marshalledEntry;
        } catch (HBaseException ex) {
            log.error("Caught HadoopException: " + ex.getMessage());
            throw new PersistenceException(ex);
        } catch (Exception ex2) {
            log.error("Caught Exception: " + ex2.getMessage());
            throw new PersistenceException(ex2);
        }
    }

    private Set<Object> loadAllKeysForTable(String table, Set<Object> keysToExclude) throws PersistenceException {
        log.debugf("In HBaseCacheStore.loadAllKeys for %s", table);

        Set<Object> allKeys = null;
        try {
            allKeys = hbf.scanForKeys(table);
        } catch (HBaseException ex) {
            log.error("HadoopException loading all keys: " + ex.getMessage());
            throw new PersistenceException(ex);
        }

        // unhash the keys
        String keyPrefix = table.equals(configuration.entryTable()) ? this.entryKeyPrefix
                : this.expirationKeyPrefix;
        Set<Object> unhashedKeys = new HashSet<Object>(allKeys.size());
        for (Object hashedKey : allKeys) {
            unhashedKeys.add(unhashKey(keyPrefix, hashedKey));
        }

        // now filter keys if necessary
        if (keysToExclude != null) {
            unhashedKeys.removeAll(keysToExclude);
        }

        return unhashedKeys;
    }

    /**
     * Purges any expired entries from the cache.
     */
    @Override
    public void purge(Executor executor, PurgeListener task) {
        log.debug("Purging expired entries.");

        try {
            // query the expiration table to find out the entries that have been expired
            long ts = ctx.getTimeService().wallClockTime();
            HBaseResultScanIterator resultIterator = hbf.readRows(
                    configuration.expirationTable(),
                    expirationKeyPrefix,
                    ts,
                    configuration.expirationColumnFamily(),
                    configuration.expirationValueField());

            Set<Object> keysToDelete = new HashSet<>();
            Set<Object> expKeysToDelete = new HashSet<>();

            while (resultIterator.hasNext()) {
                Entry<String, byte[]> entry = resultIterator.next();
                expKeysToDelete.add(entry.getKey());
                byte[] targetKeyBytes = entry.getValue();
                String targetKey = Bytes.toString(targetKeyBytes);
                keysToDelete.add(targetKey);
            }

            // remove the entries that have expired
            if (keysToDelete.size() > 0) {
                hbf.removeRows(configuration.entryTable(), keysToDelete);
            }

            // now remove any expiration rows with timestamps before now
            hbf.removeRows(configuration.expirationTable(), expKeysToDelete);

            for (Object key : keysToDelete) {
                task.entryPurged(unhashKey(entryKeyPrefix, key));
            }
        } catch (HBaseException ex) {
            log.error("HadoopException loading all keys: " + ex.getMessage());
            throw new PersistenceException(ex);
        }
    }

    @Override
    public boolean contains(Object key) {
        return load(key) != null;
    }


    @Override
    public int size() {
        return loadAllKeysForTable(configuration.entryTable(), null).size();
    }

    private void writeEntry(MarshalledEntry entry, String hashedKey) throws IOException, InterruptedException, HBaseException {
        Map<String, byte[]> qualifiersData = new HashMap<>();

        byte[] marshall = marshall(entry);
        qualifiersData.put(configuration.entryValueField(), marshall);

        Map<String, Map<String, byte[]>> familiesData = Collections.singletonMap(configuration.entryColumnFamily(),
                qualifiersData);

        hbf.addRow(configuration.entryTable(), hashedKey, familiesData);
    }

    private void writeExpireEntry(MarshalledEntry entry, String hashedKey) throws HBaseException {
        Map<String, byte[]> expValMap = Collections.singletonMap(configuration.expirationValueField(),
                Bytes.toBytes(hashedKey));
        Map<String, Map<String, byte[]>> expCfMap = Collections.singletonMap(
                configuration.expirationColumnFamily(), expValMap);

        String expKey = String.valueOf(getExpiryTime(entry.getMetadata()));
        String hashedExpKey = hashKey(this.expirationKeyPrefix, expKey);
        hbf.addRow(configuration.expirationTable(), hashedExpKey, expCfMap);
    }

    private void createTable(String name,  String family) {
        try {
            List<String> colFamilies = Collections.singletonList(family);

            // column family should only support a max of 1 version
            hbf.createTable(name, colFamilies, 1);
        } catch (HBaseException ex) {
            if (ex.getCause() instanceof TableExistsException) {
                log.infof("Not creating %s because it already exists.", name);
            } else {
                throw new PersistenceException("Got HadoopException while creating the "
                        + name + " cache store table.", ex);
            }
        }
    }

    private String hashKey(String keyPrefix, Object key) throws UnsupportedKeyTypeException {
        if (key == null) {
            return "";
        }
        if (!key2StringMapper.isSupportedType(key.getClass())) {
            throw new UnsupportedKeyTypeException(key);
        }

        return keyPrefix + key2StringMapper.getStringMapping(key);
    }

    private Object unhashKey(String keyPrefix, Object key) {
        String skey = key.toString();
        if (skey.startsWith(keyPrefix)) {
            return key2StringMapper.getKeyMapping(skey.substring(keyPrefix.length()));
        } else {
            return null;
        }
    }


    private void prepareKey2StringMapper() {
        try {
            Object mapper = Util.loadClassStrict(configuration.key2StringMapper(),
                    globalConfiguration.classLoader()).newInstance();
            if (mapper instanceof TwoWayKey2StringMapper) {
                key2StringMapper = (TwoWayKey2StringMapper) mapper;
                ((MarshallingTwoWayKey2StringMapper) key2StringMapper).setMarshaller(ctx.getMarshaller());
            }
        } catch (Exception e) {
            log.errorf("Trying to instantiate %s, however it failed due to %s", configuration.key2StringMapper(),
                    e.getClass().getName());
            throw new IllegalStateException("This should not happen.", e);
        }
        if (log.isTraceEnabled()) {
            log.tracef("Using key2StringMapper: %s", key2StringMapper.getClass().getName());
        }
    }

    private byte[] marshall(MarshalledEntry entry) throws IOException, InterruptedException {
        return ctx.getMarshaller().objectToByteBuffer(entry);
    }

    private MarshalledEntry unmarshall(byte[] bytes) throws IOException,
            ClassNotFoundException {
        return bytes == null ? null : (MarshalledEntry) ctx.getMarshaller().objectFromByteBuffer(bytes);
    }

    private Map<String, String> prepareHbaseConfiguration() {
        Map<String, String> props = new HashMap<String, String>();
        props.put("hbase.zookeeper.quorum", configuration.hbaseZookeeperQuorum());
        Integer hbaseZookeeperClientPort = configuration.hbaseZookeeperClientPort();
        props.put("hbase.zookeeper.property.clientPort", hbaseZookeeperClientPort != null ?
                String.valueOf(hbaseZookeeperClientPort) : null);
        return props;
    }

    private boolean canExpire(MarshalledEntry entry) {
        return entry.getMetadata() != null && entry.getMetadata().expiryTime() != -1;
    }

    private boolean isExpired(MarshalledEntry<?, ?> marshalledEntry, long time) {
        return marshalledEntry != null &&
                marshalledEntry.getMetadata() != null &&
                marshalledEntry.getMetadata().isExpired(time);
    }
}
