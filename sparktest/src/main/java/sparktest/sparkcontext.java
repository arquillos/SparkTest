package sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


@SuppressWarnings("serial")
class Contains implements Function<String, Boolean> {
	private String searchString;
	
	public Contains(String searchString) {
		this.searchString = searchString;
	}
	
	public Boolean call(String x) {
		return x.contains(searchString);
	}
}

public class sparkcontext {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Sparck Context Test");
		JavaSparkContext sc = new JavaSparkContext(conf);

		sc.setLogLevel("WARN");
		
		System.out.println("---------------------------");
		System.out.println("--- App Name: " + sc.appName());
		System.out.println("--- Version:  " + sc.version());
		System.out.println("---------------------------");
		System.out.println();
		
		System.out.println("---------------------------");
		JavaRDD<String> lines = sc.textFile("/var/log/messages");
		System.out.println("--- Lines read: " + lines.count());
		System.out.println("--- First line: " + lines.first());
		System.out.println("---------------------------");
		System.out.println();
		
		System.out.println("---------------------------");
		System.out.println("--- Filter");
		@SuppressWarnings("serial")
		JavaRDD<String> filteredLines = lines.filter(
				new Function<String, Boolean>() {
					public Boolean call(String x) {
						return x.contains("error");
					}
				});
		System.out.println("--- Filtered Lines.");
		for(String line: filteredLines.collect())
			System.out.println(line);
		System.out.println("---------------------------");
		System.out.println();
		
		
		System.out.println("---------------------------");
		System.out.println("--- Filter with parameters");
		JavaRDD<String> linesWithVendor = lines.filter(new Contains("Vendor"));

        System.out.println("--- Showing the lines with \"Vendor\":");
        for (String linea: linesWithVendor.collect())
            System.out.println(linea);
		System.out.println("---------------------------");
		System.out.println();

		System.out.println("---------------------------");
		System.out.println("--- Filter with lambda expression");
		JavaRDD<String> linesUsingLambda = lines.filter(string -> string.contains("error"));

        System.out.println("--- Showing the lines with \"error\"");
        for (String linea: linesUsingLambda.collect())
            System.out.println(linea);
		System.out.println("---------------------------");
        System.out.println();
        System.out.println();
 
		sc.close();
	}
}
