package sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class RDDActions {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD Actions");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.setLogLevel("WARN");
		
		System.out.println();
		System.out.println(" --- Actions ---");
		System.out.println("--- Take ");
		JavaRDD<String> lines = sc.textFile("/var/log/messages");
		System.out.println("--- 10 muestras");
		for(String line: lines.take(10))
			System.out.println(line);
		System.out.println("----------------------");
		System.out.println();
		
		System.out.println("----------------------");
		System.out.println("----------------------");
		System.out.println();
		System.out.println("----------------------");
		System.out.println("----------------------");
		System.out.println();
		System.out.println("----------------------");
		System.out.println("----------------------");
		System.out.println();
		
		sc.close();
	}
}
