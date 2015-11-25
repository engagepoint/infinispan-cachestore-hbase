package org.infinispan.loaders.hbase.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;


public class HBaseCacheStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<HBaseCacheStoreConfiguration, HBaseCacheStoreConfigurationBuilder> {

   public HBaseCacheStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, HBaseCacheStoreConfiguration.attributeDefinitionSet());
   }

   public HBaseCacheStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * Whether to automatically create the HBase table with the appropriate column families (true by
    * default).
    */
   public HBaseCacheStoreConfigurationBuilder autoCreateTable(boolean autoCreateTable) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.AUTO_CREATE_TABLE).set(autoCreateTable);
      return this;
   }

   /**
    * The column family for entries. Defaults to 'E'
    */
   public HBaseCacheStoreConfigurationBuilder entryColumnFamily(String entryColumnFamily) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.ENTRY_COLUMN_FAMILY).set(entryColumnFamily);
      return this;
   }

   /**
    * The HBase table for storing the cache entries. Defaults to 'ISPNCacheStore'
    */
   public HBaseCacheStoreConfigurationBuilder entryTable(String entryTable) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.ENTRY_TABLE).set(entryTable);
      return this;
   }

   /**
    * The field name containing the entries. Defaults to 'EV'
    */
   public HBaseCacheStoreConfigurationBuilder entryValueField(String entryValueField) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.ENTRY_VALUE_FIELD).set(entryValueField);
      return this;
   }

   /**
    * The column family for expiration metadata. Defaults to 'X'
    */
   public HBaseCacheStoreConfigurationBuilder expirationColumnFamily(String expirationColumnFamily) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.EXPIRATION_COLUMN_FAMILY).set(expirationColumnFamily);
      return this;
   }

   /**
    * The HBase table for storing the cache expiration metadata. Defaults to
    * 'ISPNCacheStoreExpiration'
    */
   public HBaseCacheStoreConfigurationBuilder expirationTable(String expirationTable) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.EXPIRATION_TABLE).set(expirationTable);
      return this;
   }

   /**
    * The field name containing the expiration metadata. Defaults to 'XV'
    */
   public HBaseCacheStoreConfigurationBuilder expirationValueField(String expirationValueField) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.EXPIRATION_VALUE_FIELD).set(expirationValueField);
      return this;
   }


   /**
    * Whether the table is shared between multiple caches. Defaults to 'false'
    */
   public HBaseCacheStoreConfigurationBuilder sharedTable(boolean sharedTable) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.SHARED_TABLE).set(sharedTable);
      return this;
   }

   /**
    * The keymapper for converting keys to strings (uses the
    */
   public HBaseCacheStoreConfigurationBuilder keyMapper(Class<? extends TwoWayKey2StringMapper> keyMapper) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.KEY2STRING_MAPPER).set(keyMapper.getName());
      return this;
   }

   /**
    * The HBase zookeeper client port. Defaults to 'localhost'
    */
   public HBaseCacheStoreConfigurationBuilder hbaseZookeeperQuorumHost(String hbaseZookeeperQuorumHost) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.HBASE_ZOOKEEPER_QUORUM).set(hbaseZookeeperQuorumHost);
      return this;
   }

   /**
    * The HBase zookeeper client port. Defaults to '2181'
    */
   public HBaseCacheStoreConfigurationBuilder hbaseZookeeperClientPort(int hbaseZookeeperClientPort) {
      this.attributes.attribute(HBaseCacheStoreConfiguration.HBASE_ZOOKEEPER_CLIENT_PORT).set(hbaseZookeeperClientPort);
      return this;
   }

   @Override
   public Builder<?> read(HBaseCacheStoreConfiguration template) {
      super.read(template);
      return this;
   }

   public HBaseCacheStoreConfiguration create() {
      return new HBaseCacheStoreConfiguration(this.attributes.protect(), this.async.create(), this.singletonStore.create());
   }

}
