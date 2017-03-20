package sparktest;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator; 


public class RDDTransformations {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD Transformations");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.setLogLevel("WARN");

		
		System.out.println();
		System.out.println("----------------------");
		System.out.println("--- Filter");
		JavaRDD<String> lines = sc.textFile("The_Adventures_of_Sherlock_Holmes.txt");
		
		@SuppressWarnings("serial")
		JavaRDD<String> filteredLines = lines.filter(
				new Function<String, Boolean>() {
					public Boolean call(String x) {
						return x.contains("error");
					}
				});
		System.out.println("--- Filtered lines:");
		for(String line: filteredLines.collect())
				System.out.println(line);
		System.out.println("----------------------");
		System.out.println();
		
		System.out.println("----------------------");
		System.out.println("--- Map");
		JavaRDD<Integer> numberList = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20));
		@SuppressWarnings("serial")
		JavaRDD<Integer> mapFiltered = numberList.map(
				new Function<Integer, Integer>() {
					public Integer call(Integer x) {
						return x * x;
					}
				});
		for(Integer line: mapFiltered.collect())
			System.out.println(line);		
		System.out.println("----------------------");
		System.out.println();
		
		System.out.println("----------------------");
		System.out.println("--- FlatMap");
		JavaRDD<String> stringList = sc.parallelize(Arrays.asList("Hola que tal", "que has echo hoy", "como lo llevas", "estamos muy listos", "no sale esto", "o puede que si"));
		@SuppressWarnings("serial")
		JavaRDD<String> flatMapList = stringList.flatMap(
				new FlatMapFunction<String, String>() {
					public Iterator<String> call(String line) {
						return Arrays.asList(line.split(" ")).iterator();
					}
				});
		for(String line: flatMapList.collect())
			System.out.println(line);
		System.out.println("----------------------");
		System.out.println();
		
		System.out.println("----------------------");
		System.out.println("--- Distinct");
		JavaRDD<String> anotherStringList = sc.parallelize(Arrays.asList("Hola", "Hola", "Adios", "Adios", "Adios", "Bye"));
		JavaRDD<String> distinctList = anotherStringList.distinct();
		System.out.println("--- Original List");
		for(String line: anotherStringList.collect())
			System.out.println(line);
		System.out.println("--- Distinct List");
		for(String line: distinctList.collect())
			System.out.println(line);
		System.out.println("----------------------");
		System.out.println();
		
		System.out.println("----------------------");
		System.out.println("--- Sample");
		JavaRDD<String> sampleRDD = anotherStringList.sample(false, 0.5);
		for(String line: sampleRDD.collect())
			System.out.println(line);
		
		System.out.println();
		System.out.println();
		System.out.println();
		System.out.println("   TWO RDDs transformations");
		System.out.println();
		JavaRDD<Integer> firstRDD = sc.parallelize(Arrays.asList(1,2,3));
		JavaRDD<Integer> secondRDD = sc.parallelize(Arrays.asList(3,4,5));
		System.out.println("--- First RDD");
		for(Integer line: firstRDD.collect())
			System.out.println(line);		
		System.out.println("--- Second RDD");
		for(Integer line: secondRDD.collect())
			System.out.println(line);		
		System.out.println("----------------------");
		System.out.println("--- Union");
		JavaRDD<Integer> unionRDD = firstRDD.union(secondRDD);
		System.out.println("--- Union RDD");
		for(Integer line: unionRDD.collect())
			System.out.println(line);		
		System.out.println();
				
		System.out.println("----------------------");
		System.out.println("--- Inersection");
		JavaRDD<Integer> intersectionRDD = firstRDD.intersection(secondRDD);
		System.out.println("--- Intersection RDD");
		for(Integer number: intersectionRDD.collect())
			System.out.println(number);
		System.out.println();
		
		System.out.println("----------------------");
		System.out.println("--- Subtract");
		JavaRDD<Integer> subtractRDD = firstRDD.subtract(secondRDD);
		System.out.println("--- Subtract RDD");
		for(Integer number: subtractRDD.collect())
			System.out.println(number);
		System.out.println();
		
		System.out.println("----------------------");
		System.out.println("--- Cartesian");
		JavaPairRDD<Integer, Integer> cartesianRDD = firstRDD.cartesian(secondRDD);
		for(Tuple2<Integer, Integer> tuple: cartesianRDD.collect())
			System.out.println("(" + tuple._1 + ", " + tuple._2 + ")");
		System.out.println();
		
		
		sc.close();
	}
}
