package org.infinispan.loaders.hbase.configuration;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

@Test(groups = "unit", testName = "loaders.hbase.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testRemoteCacheStore() throws Exception {
      String config = TestingUtil.InfinispanStartTag.LATEST  +
            "<cache-container default-cache=\"default\">\n" +
              "        <local-cache name=\"default\">\n" +
              "            <persistence passivation=\"false\">\n" +
              "                <hbase-store xmlns=\"urn:infinispan:config:store:hbase:7.2\"\n" +
              "                             auto-create-table=\"false\"\n" +
              "                             entry-table=\"ET\"\n" +
              "                             entry-column-family=\"ECF\"\n" +
              "                             entry-value-field=\"EVF\"\n" +
              "                             expiration-table=\"XT\"\n" +
              "                             expiration-column-family=\"XCF\"\n" +
              "                             expiration-value-field=\"XVF\"\n" +
              "                             shared-table=\"true\"/>\n" +
              "            </persistence>\n" +
              "        </local-cache>\n" +
              "    </cache-container>" +
            TestingUtil.INFINISPAN_END_TAG;

      HBaseCacheStoreConfiguration store = (HBaseCacheStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assert !store.autoCreateTable();
      assert store.entryColumnFamily().equals("ECF");
      assert store.entryTable().equals("ET");
      assert store.entryValueField().equals("EVF");
      assert store.expirationColumnFamily().equals("XCF");
      assert store.expirationTable().equals("XT");
      assert store.expirationValueField().equals("XVF");
      assert store.sharedTable();
   }

   private StoreConfiguration buildCacheManagerWithCacheStore(final String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assert cacheManager.getDefaultCacheConfiguration().persistence().stores().size() == 1;
      return cacheManager.getDefaultCacheConfiguration().persistence().stores().get(0);
   }
}