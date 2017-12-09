import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CitizenStatisticsTest2 {

    ApacheSQL16 subj = new ApacheSQL16();
    double delta = 1;
    @Before
    public void setup() {
        Ignite ignite = Ignition.start(getIgniteCfg());
        ignite.active(true);
        List<Citizen> citizens = generateCitizen();
        IgniteCache<Integer, Citizen> salaryCache = ignite.getOrCreateCache("salary-test");
        salaryCache.clear();
        storeData(citizens,salaryCache);
        IgniteCache<Integer, Citizen> tripsCache = ignite.getOrCreateCache("trips-test");
        tripsCache.clear();
        List<Citizen> tripCitizens = generateTripsCitizen();
        storeData(tripCitizens, tripsCache);
    }


    @Test
    public void calculate() {
        subj.setUsersPath("src/test/test.json");
        subj.setSalaryCache("salary-test");
        subj.setTripsCache("trips-test");
        Map<String, List<Double>> res = subj.init();
        assertNotNull(subj.ignite);
        assertNotNull(subj.usersPath);

        List<Double> salaries = res.get(ApacheSQL16.KEY_SALARY);
        List<Double> trips = res.get(ApacheSQL16.KEY_TRIPS);
        System.out.println(salaries.get(0));
        System.out.println(salaries.get(1));
        System.out.println(salaries.get(2));
        System.out.println(trips.get(0));
        System.out.println(trips.get(1));
        System.out.println(trips.get(2));
        assertEquals(3, salaries.size());
        assertEquals(3, trips.size());
        assertEquals((double)150, salaries.get(0), delta);
        assertEquals((double) 205, salaries.get(1), delta);
        assertEquals((double) 200, salaries.get(2), delta);
        assertEquals(Double.valueOf(1.5), trips.get(0));
        assertEquals((double) 2, trips.get(1), delta/10l);
        assertEquals(Double.valueOf(2), trips.get(2));

    }

    @After
    public void close() {
        subj.destroy();
    }




    private static IgniteConfiguration getIgniteCfg() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        setStorageCfg(cfg);
        setSalaryCache(cfg);
        setTripsCache(cfg);
        setNetwork(cfg);
        return cfg;
    }
    private static void storeData(List<Citizen> citizens, IgniteCache<Integer, Citizen> cache) {
        for (Citizen citizen :
                citizens) {
            cache.put((int)citizen.id, citizen);
        }
    }

    private static List<Citizen> generateCitizen() {
        List<Citizen> citizens = new ArrayList<>();
        int id = 0;
        for (int i = 0; i < 12; i++) {
            citizens.add(newCitizen(id++, id, i + 1, 100));
            citizens.add(newCitizen(id++, id, i + 1, 200));
            citizens.add(newCitizen(id++, id, i + 1, 300));
        }
        return citizens;
    }

    private static List<Citizen> generateTripsCitizen() {
        List<Citizen> citizens = new ArrayList<>();
        int id = 0;
        for (int i = 0; i < 12; i++) {
            citizens.add(newCitizen(id++, id, i + 1, 1));
            citizens.add(newCitizen(id++, id, i + 1, 2));
            citizens.add(newCitizen(id++, id, i + 1, 3));
        }
        return citizens;
    }

    private static Citizen newCitizen(int id, int passport, int month, int index) {
        return new Citizen(id, passport, month, index);
    }

    static void setStorageCfg(IgniteConfiguration cfg) {
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        cfg.setDataStorageConfiguration(storageCfg);
        cfg.setLocalHost("localhost");
        cfg.setDeploymentMode(SHARED);
        cfg.setPeerClassLoadingEnabled(false);
    }

    static void setSalaryCache(IgniteConfiguration cfg) {
        CacheConfiguration<AffinityKey<Long>, Citizen> salaryCacheCfg = new CacheConfiguration();
        salaryCacheCfg.setName("salary-test");
        salaryCacheCfg.setAtomicityMode(TRANSACTIONAL);
        salaryCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        salaryCacheCfg.setRebalanceMode(SYNC);
        salaryCacheCfg.setIndexedTypes(Integer.class, Citizen.class);
        salaryCacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setCacheConfiguration(salaryCacheCfg);
    }

    static void setTripsCache(IgniteConfiguration cfg) {
        CacheConfiguration<AffinityKey<Long>, Citizen> tripsCacheCfg = new CacheConfiguration();
        tripsCacheCfg.setName("trips-test");
        tripsCacheCfg.setAtomicityMode(TRANSACTIONAL);
        tripsCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        tripsCacheCfg.setRebalanceMode(SYNC);
        tripsCacheCfg.setIndexedTypes(Integer.class, Citizen.class);
        tripsCacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setCacheConfiguration(tripsCacheCfg);
    }

    static void setNetwork(IgniteConfiguration cfg) {
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder tcMp = new TcpDiscoveryMulticastIpFinder();
        tcMp.setAddresses(Arrays.asList("localhost"));
        discoSpi.setIpFinder(tcMp);
        cfg.setDiscoverySpi(discoSpi);
    }

}