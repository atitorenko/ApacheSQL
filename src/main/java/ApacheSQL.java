//import lombok.AllArgsConstructor;
//import lombok.Getter;
//import lombok.NoArgsConstructor;
//import lombok.Setter;
//import org.apache.ignite.Ignite;
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.apache.ignite.lang.IgniteOutClosure;
//import org.apache.ignite.spark.JavaIgniteContext;
//import org.apache.ignite.spark.JavaIgniteRDD;
//import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
//import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//import java.time.LocalDate;
//import java.util.*;
//
///**
// * Class for data quering and computing.
// */
//import static org.apache.spark.sql.functions.*;
//
//@Setter
//@Getter
//@NoArgsConstructor
//@AllArgsConstructor
//public class ApacheSQL {
//    /**
//     * Path to csv file with users personal data.
//     */
//    String usersPath = "src/main/resources/people.csv";
//    SparkConf conf;
//    SparkSession spark;
//    JavaSparkContext sc;
//    IgniteOutClosure cfg;
//    JavaIgniteContext<Integer, Citizen> ic;
//    Ignite ignite;
//
//    static final String KEY_SALARY = "salary";
//    static final String KEY_TRIPS = "trips";
//
//    static final String AGE1 = "under_twenty";
//    static final String AGE2 = "twenty_2_fifty";
//    static final String AGE3 = "over_fifty";
//
//    /**
//     * Initialize configuration.
//     */
//    public void init() {
//        conf = new SparkConf()
//                .setAppName("test")
//                .setMaster("local[*]");
//
//        spark = SparkSession
//                .builder()
//                .config(conf)
//                .getOrCreate();
//        sc = new JavaSparkContext(spark.sparkContext());
//        cfg = (IgniteOutClosure) () -> {
//            TcpDiscoverySpi spi = new TcpDiscoverySpi();
//            TcpDiscoveryMulticastIpFinder tcMp = new TcpDiscoveryMulticastIpFinder();
//            tcMp.setAddresses(Arrays.asList("localhost"));
//            spi.setIpFinder(tcMp);
//            IgniteConfiguration cfg1 = new IgniteConfiguration();
//            cfg1.setClientMode(true);
//            cfg1.setDiscoverySpi(spi);
//            return cfg1;
//        };
//
//        ic = new JavaIgniteContext<Integer, Citizen>(sc, cfg, false);
//        ignite = ic.ignite();
//    }
//
//    /**
//     * Call computing methods.
//     * @return res Map. key - cache type(salary, trips), value - list of computing values for age categories.
//     */
//    public Map<String, List<Double>> run() {
//        Map<String, List<Double>> res = new HashMap<>();
//        res.put(KEY_SALARY,  processSalary(ic, spark));
//        res.put(KEY_TRIPS,   processTrips(ic, spark));
//        return res;
//    }
//
//    /**
//     * Stop ignite instance.
//     */
//    public void destroy() {
//        ignite.close();
//    }
//
//    /**
//     * Compute average salary for each age category.
//     * @param ic ignite context.
//     * @param spark spark session.
//     * @return list of aggregating values for three age category - <20 years, 20-50 years, >50 years.
//     */
//    List<Double> processSalary(JavaIgniteContext<Integer, Citizen> ic, SparkSession spark) {
//        int today = LocalDate.now().getYear();
//        JavaIgniteRDD<Integer, Citizen> sharedRDD = ic.<Integer, Citizen>fromCache("salary");
//        Dataset<Row> salaryDf = sharedRDD.sql("select * from Citizen");
//        Dataset<Row> usersDf =  spark.read().format("csv").option("header", true).csv(usersPath);
//        Dataset<Row> avgSalary20 = usersDf.filter(year(col("birth_date")).gt(today - 20))//<20
//                .join(salaryDf, salaryDf.col("PASSPORTNUMBER").equalTo(usersDf.col("passport")))
//                .groupBy(col("PASSPORTNUMBER"))
//                .agg(avg(salaryDf.col("INDEX")).name("average")).agg(avg("average").name("avg_sal_20"));
//
//        Dataset<Row> avgSalary20_50 = usersDf.filter(year(col("birth_date")).between(today - 50, today - 21 ))//>20 && <50
//                .join(salaryDf, salaryDf.col("PASSPORTNUMBER").equalTo(usersDf.col("passport")))
//                .groupBy(col("PASSPORTNUMBER"))
//                .agg(avg(salaryDf.col("INDEX")).name("average")).agg(avg("average").name("avg_sal_20_50"));
//
//        Dataset<Row> avgSalary50 = usersDf.filter(year(col("birth_date")).lt(today - 51))//>50
//                .join(salaryDf, salaryDf.col("PASSPORTNUMBER").equalTo(usersDf.col("passport")))
//                .groupBy(col("PASSPORTNUMBER"))
//                .agg(avg(salaryDf.col("INDEX")).name("average")).agg(avg("average").name("avg_sal_50"));
//
//
//        Double sal20 = avgSalary20.first().isNullAt(0) ? 0 : avgSalary20.first().getDouble(0);
//        Double sal20_50 = avgSalary20_50.first().isNullAt(0) ? 0 : avgSalary20_50.first().getDouble(0);
//        Double sal50 = avgSalary50.first().isNullAt(0) ? 0 : avgSalary50.first().getDouble(0);
//        System.out.println(AGE1 + "," + sal20);
//        System.out.println(AGE2 + "," + sal20_50);
//        System.out.println(AGE3 + "," + sal50);
//        return Arrays.asList(sal20, sal20_50, sal50);
//    }
//
//    /**
//     * Compute average aboard trips for each age category.
//     * @param ic ignite context.
//     * @param spark spark session.
//     * @return list of aggregating values for three age category - <20 years, 20-50 years, >50 years.
//     */
//    List<Double> processTrips(JavaIgniteContext<Integer, Citizen> ic, SparkSession spark) {
//        int today = LocalDate.now().getYear();
//        JavaIgniteRDD<Integer, Citizen> sharedRDD = ic.<Integer, Citizen>fromCache("trips");
//        Dataset<Row> tripsDf = sharedRDD.sql("select * from Citizen");
//        Dataset<Row> usersDf =  spark.read().format("csv").option("header", true).csv("src/main/resources/people.csv");
//        Dataset<Row> avgTrips20 = usersDf.filter(year(col("birth_date")).gt(today - 20))//<20
//                .join(tripsDf, tripsDf.col("PASSPORTNUMBER").equalTo(usersDf.col("passport")))
//                .groupBy(col("PASSPORTNUMBER"))
//                .agg(avg(tripsDf.col("INDEX")).name("average")).agg(avg("average").name("avg_trp_20"));
//
//        Dataset<Row> avgTrips20_50 = usersDf.filter(year(col("birth_date")).between(today - 50, today - 21 ))//>20 && <50
//                .join(tripsDf, tripsDf.col("PASSPORTNUMBER").equalTo(usersDf.col("passport")))
//                .groupBy(col("PASSPORTNUMBER"))
//                .agg(avg(tripsDf.col("INDEX")).name("average")).agg(avg("average").name("avg_trp_20_50"));
//
//        Dataset<Row> avgTrips50 = usersDf.filter(year(col("birth_date")).lt(today - 51))//>50
//                .join(tripsDf, tripsDf.col("PASSPORTNUMBER").equalTo(usersDf.col("passport")))
//                .groupBy(col("PASSPORTNUMBER"))
//                .agg(avg(tripsDf.col("INDEX")).name("average")).agg(avg("average").name("avg_trp_20_50"));
//
//
//        Double trp20 = avgTrips20.first().isNullAt(0) ? 0 : avgTrips20.first().getDouble(0);
//        Double trp20_50 = avgTrips20_50.first().isNullAt(0) ? 0 : avgTrips20_50.first().getDouble(0);
//        Double trp50 = avgTrips50.first().isNullAt(0) ? 0 : avgTrips50.first().getDouble(0);
//
//        System.out.println(AGE1 + "," + trp20);
//        System.out.println(AGE2 + "," + trp20_50);
//        System.out.println(AGE3 + "," + trp50);
//
//        return Arrays.asList(trp20, trp20_50, trp50);
//    }
//
//}