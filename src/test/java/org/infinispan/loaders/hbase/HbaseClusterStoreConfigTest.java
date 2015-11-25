package org.infinispan.loaders.hbase;

import org.infinispan.Cache;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.jgroups.util.Util.assertEquals;

@Test(testName = "persistence.redis.HbaseClusterStoreConfigTest", groups = "functional")
public class HbaseClusterStoreConfigTest extends AbstractInfinispanTest {
    private static final String CACHE_LOADER_CONFIG = "hbase-cachestore-ispn.xml";

    @BeforeTest(alwaysRun = true)
    public void beforeTest()
            throws IOException {
        System.out.println("HbaseClusterStoreConfigTest:Setting up");
    }

    public void simpleTest() throws Exception {
        withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(CACHE_LOADER_CONFIG)) {
            @Override
            public void call() {
                Cache<Object, Object> cache = cm.getCache();
                CacheLoader cacheLoader = TestingUtil.getCacheLoader(cache);
                assert cacheLoader != null;
                assert cacheLoader instanceof HBaseCacheStore;

                cache.put("k", "v");

                assertEquals(1, cm.getCache().size());
                cache.stop();
            }
        });
    }

    @AfterTest(alwaysRun = true)
    public void afterTest() {
        System.out.println("HbaseClusterStoreConfigTest:Tearing down");
    }
}
