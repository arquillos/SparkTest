package sparktest;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

// Need a working Hadoop configured system

public class WordCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.setLogLevel("WARN");

		JavaRDD<String> file = sc.textFile("The_Adventures_of_Sherlock_Holmes.txt");
		
		// Split up into words.
		@SuppressWarnings("serial")
		JavaRDD<String> words = file.flatMap(
				new FlatMapFunction<String, String>() {
					public Iterator<String> call(String line) {
						return Arrays.asList(line.split(" ")).iterator();
					}
				});
		
		// Transform into pairs and count.
		@SuppressWarnings("serial")
		JavaPairRDD<String, Integer> ones = words.mapToPair(
					new PairFunction<String, String, Integer>() {
						public Tuple2<String, Integer> call (String x) {
							return new Tuple2<>(x, 1);
						}
					});

		@SuppressWarnings("serial")
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});
		
		// Writing output
		List<Tuple2<String, Integer>> output = counts.collect();
		for(Tuple2<?,?> tuple: output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		
		
		sc.close();
	}

}
