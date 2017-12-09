import java.util.List;
import java.util.Map;

/**
 * Main class application. Initiate computing and print result.
 */
public class Main {

    public static void main(String[] args) {
//        ApacheSQL apacheSQL = new ApacheSQL();
//        apacheSQL.init();
//        Map<String, List<Double>> res = apacheSQL.run();
//        List<Double> salary = res.get(ApacheSQL.KEY_SALARY);
//        System.out.println(ApacheSQL.AGE1 + "," + salary.get(0));
//        System.out.println(ApacheSQL.AGE2 + "," + salary.get(1));
//        System.out.println(ApacheSQL.AGE3 + "," + salary.get(2));
//
//        List<Double> trips = res.get(ApacheSQL.KEY_TRIPS);
//        System.out.println(ApacheSQL.AGE1 + "," + trips.get(0));
//        System.out.println(ApacheSQL.AGE2 + "," + trips.get(1));
//        System.out.println(ApacheSQL.AGE3 + "," + trips.get(2));
//        apacheSQL.destroy();

        ApacheSQL16 apacheSQL16 = new ApacheSQL16();
        Map<String, List<Double>> res = apacheSQL16.init();
        List<Double> salary = res.get(ApacheSQL16.KEY_SALARY);
        System.out.println(ApacheSQL16.AGE1 + "," + salary.get(0));
        System.out.println(ApacheSQL16.AGE2 + "," + salary.get(1));
        System.out.println(ApacheSQL16.AGE3 + "," + salary.get(2));

        List<Double> trips = res.get(ApacheSQL16.KEY_TRIPS);
        System.out.println(ApacheSQL16.AGE1 + "," + trips.get(0));
        System.out.println(ApacheSQL16.AGE2 + "," + trips.get(1));
        System.out.println(ApacheSQL16.AGE3 + "," + trips.get(2));
        apacheSQL16.destroy();
    }

}
