package org.infinispan.loaders.hbase.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.hbase.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testHBaseCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(HBaseCacheStoreConfigurationBuilder.class)
         .autoCreateTable(false)
         .entryColumnFamily("ECF")
         .entryTable("ET")
         .entryValueField("EVF")
         .expirationColumnFamily("XCF")
         .expirationTable("XT")
         .expirationValueField("XVF")
         .sharedTable(true)
      .fetchPersistentState(true).async().enable();
      Configuration configuration = b.build();
      HBaseCacheStoreConfiguration store = (HBaseCacheStoreConfiguration) configuration.persistence().stores().get(0);
      assert !store.autoCreateTable();
      assert store.entryColumnFamily().equals("ECF");
      assert store.entryTable().equals("ET");
      assert store.entryValueField().equals("EVF");
      assert store.expirationColumnFamily().equals("XCF");
      assert store.expirationTable().equals("XT");
      assert store.expirationValueField().equals("XVF");
      assert store.sharedTable();
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.persistence().addStore(HBaseCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      HBaseCacheStoreConfiguration store2 = (HBaseCacheStoreConfiguration) configuration2.persistence().stores().get(0);
      assert !store2.autoCreateTable();
      assert store2.entryColumnFamily().equals("ECF");
      assert store2.entryTable().equals("ET");
      assert store2.entryValueField().equals("EVF");
      assert store2.expirationColumnFamily().equals("XCF");
      assert store2.expirationTable().equals("XT");
      assert store2.expirationValueField().equals("XVF");
      assert store2.sharedTable();
      assert store2.fetchPersistentState();
      assert store2.async().enabled();
   }
}
