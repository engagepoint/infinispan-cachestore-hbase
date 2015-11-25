package org.infinispan.loaders.hbase;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.hbase.configuration.HBaseCacheStoreConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.hbase.HBaseCacheStoreTest")
public class HBaseCacheStoreTest extends BaseStoreTest {


   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      HBaseCacheStoreConfigurationBuilder storeConfigurationBuilder =
              builder.persistence().addStore(HBaseCacheStoreConfigurationBuilder.class);
      storeConfigurationBuilder.autoCreateTable(true)
              .entryColumnFamily("ECF")
              .entryTable("ET")
              .entryValueField("EVF")
              .expirationColumnFamily("XCF")
              .expirationTable("XT")
              .expirationValueField("XVF")
              .sharedTable(true);
      HBaseCacheStore store = new HBaseCacheStore();
      store.init(createContext(builder.build()));

      return store;
   }

   @Override
   protected void purgeExpired(String... expiredKeys) throws Exception {
      //TODO This test is skipped because replaces the old value with the new one in expiration table
   }
}
