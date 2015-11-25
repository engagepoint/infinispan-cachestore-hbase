package org.infinispan.loaders.hbase.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.hbase.HBaseCacheStore;
import org.infinispan.persistence.keymappers.MarshalledValueOrPrimitiveMapper;

@BuiltBy(HBaseCacheStoreConfigurationBuilder.class)
@ConfigurationFor(HBaseCacheStore.class)
public class HBaseCacheStoreConfiguration extends AbstractStoreConfiguration {

    private final Attribute<Boolean> autoCreateTable;
    private final Attribute<String> entryColumnFamily;
    private final Attribute<String> entryTable;
    private final Attribute<String> entryValueField;
    private final Attribute<String> expirationColumnFamily;
    private final Attribute<String> expirationTable;
    private final Attribute<String> expirationValueField;
    private final Attribute<Boolean> sharedTable;
    private final Attribute<String> key2StringMapper;
    private final Attribute<String> hbaseZookeeperQuorum;
    private final Attribute<Integer> hbaseZookeeperClientPort;

    static final AttributeDefinition<String> ENTRY_TABLE = AttributeDefinition.builder("entryTable", null, String.class).build();
    static final AttributeDefinition<String> EXPIRATION_TABLE = AttributeDefinition.builder("expirationTable", null, String.class).build();
    static final AttributeDefinition<Boolean> AUTO_CREATE_TABLE = AttributeDefinition.builder("autoCreateTable", false, Boolean.class).build();
    static final AttributeDefinition<String> ENTRY_COLUMN_FAMILY = AttributeDefinition.builder("entryColumnFamily", null, String.class).build();
    static final AttributeDefinition<String> ENTRY_VALUE_FIELD = AttributeDefinition.builder("entryValueField", null, String.class).build();
    static final AttributeDefinition<String> EXPIRATION_COLUMN_FAMILY = AttributeDefinition.builder("expirationColumnFamily", null, String.class).build();
    static final AttributeDefinition<String> EXPIRATION_VALUE_FIELD = AttributeDefinition.builder("expirationValueField", null, String.class).build();
    static final AttributeDefinition<Boolean> SHARED_TABLE = AttributeDefinition.builder("sharedTable", false, Boolean.class).build();
    static final AttributeDefinition<String> KEY2STRING_MAPPER = AttributeDefinition.builder("key2StringMapper",
            MarshalledValueOrPrimitiveMapper.class.getName()).immutable().build();
    static final AttributeDefinition<String> HBASE_ZOOKEEPER_QUORUM = AttributeDefinition.builder("hbaseZookeeperQuorum", null, String.class).build();
    static final AttributeDefinition<Integer> HBASE_ZOOKEEPER_CLIENT_PORT = AttributeDefinition.builder("hbaseZookeeperClientPort", null, Integer.class).build();


    public static AttributeSet attributeDefinitionSet() {
        return new AttributeSet(HBaseCacheStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(),
                ENTRY_TABLE, EXPIRATION_TABLE, AUTO_CREATE_TABLE, ENTRY_COLUMN_FAMILY, ENTRY_VALUE_FIELD, EXPIRATION_COLUMN_FAMILY,
                EXPIRATION_VALUE_FIELD, SHARED_TABLE, KEY2STRING_MAPPER, HBASE_ZOOKEEPER_QUORUM, HBASE_ZOOKEEPER_CLIENT_PORT);
    }

    public HBaseCacheStoreConfiguration(AttributeSet attributes,
                                        AsyncStoreConfiguration async,
                                        SingletonStoreConfiguration singletonStore
    ) {
        super(attributes, async, singletonStore);

        this.autoCreateTable = attributes.attribute(AUTO_CREATE_TABLE);
        this.entryColumnFamily = attributes.attribute(ENTRY_COLUMN_FAMILY);
        this.entryTable = attributes.attribute(ENTRY_TABLE);
        this.entryValueField = attributes.attribute(ENTRY_VALUE_FIELD);
        this.expirationColumnFamily = attributes.attribute(EXPIRATION_COLUMN_FAMILY);
        this.expirationTable = attributes.attribute(EXPIRATION_TABLE);
        this.expirationValueField = attributes.attribute(EXPIRATION_VALUE_FIELD);
        this.sharedTable = attributes.attribute(SHARED_TABLE);
        this.key2StringMapper = attributes.attribute(KEY2STRING_MAPPER);
        this.hbaseZookeeperQuorum = attributes.attribute(HBASE_ZOOKEEPER_QUORUM);
        this.hbaseZookeeperClientPort = attributes.attribute(HBASE_ZOOKEEPER_CLIENT_PORT);
    }

    public String key2StringMapper() {
        return key2StringMapper.get();
    }

    public boolean autoCreateTable() {
        return autoCreateTable.get();
    }

    public String entryColumnFamily() {
        return entryColumnFamily.get();
    }

    public String entryTable() {
        return entryTable.get();
    }

    public String entryValueField() {
        return entryValueField.get();
    }

    public String expirationColumnFamily() {
        return expirationColumnFamily.get();
    }

    public String expirationTable() {
        return expirationTable.get();
    }

    public String expirationValueField() {
        return expirationValueField.get();
    }

    public boolean sharedTable() {
        return sharedTable.get();
    }

    public String hbaseZookeeperQuorum() {
        return hbaseZookeeperQuorum.get();
    }

    public Integer hbaseZookeeperClientPort() {
        return hbaseZookeeperClientPort.get();
    }

}
