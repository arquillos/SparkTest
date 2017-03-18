package sparktest;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDCreation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD Creation");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.setLogLevel("WARN");
		
		System.out.println("--- RDD creation from string list.");
		JavaRDD<String> rdd = sc.parallelize(Arrays.asList("uno","dos","tres","cuatro","cinco","seis","siete","ocho","nueve","diez"));
		System.out.println("--- RDD: ");
		for(String line: rdd.collect())
			System.out.println(line);
		
		sc.close();
	}

}
