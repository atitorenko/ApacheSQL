import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.spark.IgniteContext;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.time.LocalDate;
import java.util.*;

/**
 * Class for data quering and computing.
 */
import static org.apache.spark.sql.functions.*;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ApacheSQL16 {
    /**
     * Path to csv file with users personal data.
     */
    String usersPath = "file:///home/cloudera/IdeaProjects/ApacheSql/src/main/resources/citizens.json";

    String salaryCache = "salary";
    String tripsCache = "trips";
    static final String KEY_SALARY = "salary";
    static final String KEY_TRIPS = "trips";

    static final String AGE1 = "under_twenty";
    static final String AGE2 = "twenty_2_fifty";
    static final String AGE3 = "over_fifty";
    Ignite ignite;
    /**
     * Initialize configuration.
     */
    public Map<String, List<Double>> init() {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "test");
        IgniteOutClosure cfg = (IgniteOutClosure) () -> {
            TcpDiscoverySpi spi = new TcpDiscoverySpi();
            TcpDiscoveryMulticastIpFinder tcMp = new TcpDiscoveryMulticastIpFinder();
            tcMp.setAddresses(Arrays.asList("localhost"));
            spi.setIpFinder(tcMp);
            IgniteConfiguration cfg1 = new IgniteConfiguration();
            cfg1.setClientMode(true);
            cfg1.setDiscoverySpi(spi);
            return cfg1;
        };

        JavaIgniteContext<Integer, Citizen> ic = new JavaIgniteContext<Integer, Citizen>(sc, cfg, false);
        ignite = ic.ignite();
        Map<String, List<Double>> res = new HashMap<>();
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame passport = sqlContext.read().json(usersPath);
        res.put(KEY_TRIPS,   processTrips(ic, passport, sqlContext));

        res.put(KEY_SALARY,  processSalary(ic, passport, sqlContext));

        return res;
    }

    /**
     * Compute average salary for each age category.
     * @param ic ignite context.
     * @param spark spark session.
     * @return list of aggregating values for three age category - <20 years, 20-50 years, >50 years.
     */
    List<Double> processSalary(JavaIgniteContext<Integer, Citizen> ic, DataFrame passport, SQLContext sqlContext) {
        int today = LocalDate.now().getYear();
        JavaIgniteRDD<Integer, Citizen> sharedRDD = ic.<Integer, Citizen>fromCache(salaryCache);

        DataFrame salaryDf = sqlContext.createDataFrame(sharedRDD.values(), Citizen.class);

        DataFrame avgSalary20 = passport.filter(year(col("birth_date")).gt(today - 20))//<20
                .join(salaryDf, salaryDf.col("passportNumber").equalTo(passport.col("passport")))
                .groupBy(col("passportNumber"))
                .agg(avg(salaryDf.col("index")).alias("average")).agg(avg("average").alias("avg_sal_20"));

        DataFrame avgSalary20_50 = passport.filter(year(col("birth_date")).between(today - 50, today - 21 ))//>20 && <50
                .join(salaryDf, salaryDf.col("passportNumber").equalTo(passport.col("passport")))
                .groupBy(col("passportNumber"))
                .agg(avg(salaryDf.col("index")).alias("average")).agg(avg("average").alias("avg_sal_20_50"));

        DataFrame  avgSalary50 = passport.filter(year(col("birth_date")).lt(today - 51))//>50
                .join(salaryDf, salaryDf.col("passportNumber").equalTo(passport.col("passport")))
                .groupBy(col("passportNumber"))
                .agg(avg(salaryDf.col("index")).alias("average")).agg(avg("average").alias("avg_sal_50"));


        Double sal20 = avgSalary20.first().isNullAt(0) ? 0 : avgSalary20.first().getDouble(0);
        Double sal20_50 = avgSalary20_50.first().isNullAt(0) ? 0 : avgSalary20_50.first().getDouble(0);
        Double sal50 = avgSalary50.first().isNullAt(0) ? 0 : avgSalary50.first().getDouble(0);
//        System.out.println(AGE1 + "," + sal20);
//        System.out.println(AGE2 + "," + sal20_50);
//        System.out.println(AGE3 + "," + sal50);
        return Arrays.asList(sal20, sal20_50, sal50);
    }
    /**
     * Compute average aboard trips for each age category.
     * @param ic ignite context.
     * @param spark spark session.
     * @return list of aggregating values for three age category - <20 years, 20-50 years, >50 years.
     */
    List<Double> processTrips(JavaIgniteContext<Integer, Citizen> ic, DataFrame passport, SQLContext sqlContext) {
        int today = LocalDate.now().getYear();
        JavaIgniteRDD<Integer, Citizen> sharedRDD = ic.<Integer, Citizen>fromCache(tripsCache);
        DataFrame tripsDf = sqlContext.createDataFrame(sharedRDD.values(), Citizen.class);
        DataFrame avgTrips20 = passport.filter(year(col("birth_date")).gt(today - 20))//<20
                .join(tripsDf, tripsDf.col("passportNumber").equalTo(passport.col("passport")))
                .groupBy(col("passportNumber"))
                .agg(avg(tripsDf.col("index")).alias("average")).agg(avg("average").alias("avg_trp_20"));

        DataFrame avgTrips20_50 = passport.filter(year(col("birth_date")).between(today - 50, today - 21 ))//>20 && <50
                .join(tripsDf, tripsDf.col("passportNumber").equalTo(passport.col("passport")))
                .groupBy(col("passportNumber"))
                .agg(avg(tripsDf.col("index")).alias("average")).agg(avg("average").alias("avg_trp_20_50"));

        DataFrame avgTrips50 = passport.filter(year(col("birth_date")).lt(today - 51))//>50
                .join(tripsDf, tripsDf.col("passportNumber").equalTo(passport.col("passport")))
                .groupBy(col("passportNumber"))
                .agg(avg(tripsDf.col("index")).alias("average")).agg(avg("average").alias("avg_trp_20_50"));


        Double trp20 = avgTrips20.first().isNullAt(0) ? 0 : avgTrips20.first().getDouble(0);
        Double trp20_50 = avgTrips20_50.first().isNullAt(0) ? 0 : avgTrips20_50.first().getDouble(0);
        Double trp50 = avgTrips50.first().isNullAt(0) ? 0 : avgTrips50.first().getDouble(0);

//        System.out.println(AGE1 + "," + trp20);
//        System.out.println(AGE2 + "," + trp20_50);
//        System.out.println(AGE3 + "," + trp50);

        return Arrays.asList(trp20, trp20_50, trp50);
    }
    /**
     * Stop ignite instance.
     */
    public void destroy() {
        ignite.close();
    }


}