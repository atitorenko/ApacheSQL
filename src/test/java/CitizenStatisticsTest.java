//import org.apache.ignite.Ignite;
//import org.apache.ignite.IgniteCache;
//import org.apache.ignite.Ignition;
//import org.apache.ignite.configuration.CacheConfiguration;
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.apache.ignite.internal.util.typedef.F;
//import org.apache.ignite.lang.IgniteOutClosure;
//import org.apache.ignite.spark.JavaIgniteContext;
//import org.apache.ignite.spark.JavaIgniteRDD;
//import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
//import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
//import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.sql.Dataset;
//import org.junit.Before;
//import scala.Tuple2;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//
//import java.util.List;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//
//public class CitizenStatisticsTest {
//    private static final String ENTITY_CACHE_NAME = "citizen-cache";
//    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
//    private static final int KEYS_CNT = 300;
//
//    /** (String, Entity) pair to Entity value function. */
//    private static final Function<Tuple2<String, Citizen>, Citizen> STR_ENTITY_PAIR_TO_ENTITY_F =
//            new PairToValueFunction<>();
//
//    /** Integer to entity function. */
//    private static final PairFunction<Integer, String, Citizen> INT_TO_ENTITY_F =
//            new PairFunction<Integer, String, Citizen>() {
//                @Override public Tuple2<String, Citizen> call(Integer i) throws Exception {
//                    return new Tuple2<>(String.valueOf(i), new Citizen(i, 100000 + i, i * 100, i * 200));
//                }
//            };
//    private static final PairFunction<Integer, String, String> TO_PAIR_F = new PairFunction<Integer, String, String>() {
//        /** {@inheritDoc} */
//        @Override public Tuple2<String, String> call(Integer i) {
//            return new Tuple2<>(String.valueOf(i), "val" + i);
//        }
//    };
//
//    protected void beforeTest() throws Exception {
//        Ignition.ignite("grid-0").cache(ENTITY_CACHE_NAME).clear();
//    }
//
//    protected void afterTest() throws Exception {
//        Ignition.stop("client", false);
//    }
//
//    protected void beforeTestsStarted() throws Exception {
//            Ignition.start(getConfiguration("grid-0", false));
//    }
//    protected void afterTestsStopped() throws Exception {
//            Ignition.stop("grid-0", false);
//    }
//
//    public void testStoreDataToIgnite() throws Exception {
//        JavaSparkContext sc = new JavaSparkContext("local[*]", "test");
//
//        try {
//            JavaIgniteContext<String, String> ic = new JavaIgniteContext<>(sc, new IgniteConfigProvider());
//
//            ic.fromCache(ENTITY_CACHE_NAME)
//                    .savePairs(sc.parallelize(F.range(0, KEYS_CNT), 2).mapToPair(TO_PAIR_F));
//
//            Ignite ignite = Ignition.ignite("grid-0");
//
//            IgniteCache<String, String> cache = ignite.cache(ENTITY_CACHE_NAME);
//
//            for (int i = 0; i < KEYS_CNT; i++) {
//                String val = cache.get(String.valueOf(i));
//
//                assertNotNull("Value was not put to cache for key: " + i, val);
//                assertEquals("Invalid value stored for key: " + i, "val" + i, val);
//            }
//        }
//        finally {
//            sc.stop();
//        }
//    }
//
//    public void testQueryObjectsFromIgnite() throws Exception {
//        JavaSparkContext sc = new JavaSparkContext("local[*]", "test");
//
//        try {
//            JavaIgniteContext<Integer, Citizen> ic = new JavaIgniteContext<>(sc, new IgniteConfigProvider());
//
//            JavaIgniteRDD<Integer, Citizen> cache = ic.fromCache(ENTITY_CACHE_NAME);
//
//            cache.savePairs(sc.parallelize(F.range(0, 1001), 2).mapToPair(INT_TO_ENTITY_F));
//
//            List<Citizen> res = cache.objectSql("Entity", "name = ? and salary = ?", "name50", 5000)
//                    .map(STR_ENTITY_PAIR_TO_ENTITY_F).collect();
//
//            assertEquals("Invalid result length", 1, res.size());
//            assertEquals("Invalid result", 50, res.get(0).id());
//            assertEquals("Invalid result", "name50", res.get(0).name());
//            assertEquals("Invalid result", 5000, res.get(0).salary());
//            assertEquals("Invalid count", 500, cache.objectSql("Entity", "id > 500").count());
//        }
//        finally {
//            sc.stop();
//        }
//    }
//
//
//    private static IgniteConfiguration getConfiguration(String igniteInstanceName, boolean client) throws Exception {
//        IgniteConfiguration cfg = new IgniteConfiguration();
//        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
//        discoSpi.setIpFinder(IP_FINDER);
//        cfg.setDiscoverySpi(discoSpi);
//        cfg.setCacheConfiguration(
//                cacheConfiguration(ENTITY_CACHE_NAME, Integer.class, Citizen.class));
//
//        cfg.setClientMode(client);
//
//        cfg.setIgniteInstanceName(igniteInstanceName);
//
//        return cfg;
//    }
//
//    private static CacheConfiguration<Object, Object> cacheConfiguration(String name, Class<?> clsK, Class<?> clsV) {
//        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("default");
//        ccfg.setBackups(1);
//        ccfg.setName(name);
//        ccfg.setIndexedTypes(clsK, clsV);
//        return ccfg;
//    }
//
//    static class IgniteConfigProvider implements IgniteOutClosure<IgniteConfiguration> {
//        /** {@inheritDoc} */
//        @Override public IgniteConfiguration apply() {
//            try {
//                return getConfiguration("client", true);
//            }
//            catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }
//
//    static class PairToValueFunction<K, V> implements Function<Tuple2<K, V>, V> {
//        public V call(Tuple2<K, V> t) throws Exception {
//            return t._2();
//        }
//    }
//}
