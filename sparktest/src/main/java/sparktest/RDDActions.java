package sparktest;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;


@SuppressWarnings("serial")
class AvgCount implements Serializable {
	public int total;
	public int num;
	
	public AvgCount(int total, int num) {
		this.total = total;
		this.num = num; 
	}
	
	public double avg() {
		return total / (double) num;
	}
}


public class RDDActions {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD Actions");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.setLogLevel("WARN");
		
		System.out.println();
		System.out.println(" --- Actions ---");
		System.out.println("--- Take ");
		JavaRDD<String> lines = sc.textFile("The_Adventures_of_Sherlock_Holmes.txt");
		System.out.println("--- 10 first lines");
		for(String line: lines.take(10))
			System.out.println(line);
		System.out.println("----------------------");
		System.out.println();
		
		JavaRDD<Integer> example = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20));
		System.out.println("--- RDD original");
		System.out.print("[");
		for(Integer number: example.collect())
			System.out.print(number + ", ");
		System.out.print("]");
		System.out.println();
		
		System.out.println("--- Reduce");
		@SuppressWarnings("serial")
		Integer reducedRDD = example.reduce(
				new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer arg0, Integer arg1) throws Exception {
						return arg0 + arg1;
					}
				});
		System.out.println("Reduced RDD (+): " + reducedRDD);
		System.out.println();
		
		System.out.println("--- Fold (Zero Value: 0)");
		@SuppressWarnings("serial")
		Integer foldedRDD = example.fold(0, 
				new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer arg0, Integer arg1) throws Exception {
						return arg0 + arg1;
					}
				});
		System.out.println("Folded RDD (+): " + foldedRDD);
		System.out.println();

		System.out.println("--- Agregate");
		@SuppressWarnings("serial")
		Function2<AvgCount, Integer, AvgCount> addAndCount =
				new Function2<AvgCount, Integer, AvgCount>() {
					public AvgCount call(AvgCount a, Integer x) {
						a.total += x;
						a.num += 1;
						
						return a;
					}
				};
		
		@SuppressWarnings("serial")
		Function2<AvgCount, AvgCount, AvgCount> combine =
				new Function2<AvgCount, AvgCount, AvgCount>() {
					public AvgCount call(AvgCount a, AvgCount b) {
						a.total += b.total;
						a.num += b.num;
						
						return a;
					}
				};
		
		AvgCount initial = new AvgCount(0, 0);
		AvgCount result = example.aggregate(initial, addAndCount, combine);
		System.out.println("--- Average result: " + result.avg());
		System.out.println();
		
		System.out.println("----------------------");
		System.out.println();
		System.out.println("----------------------");
		System.out.println("----------------------");
		System.out.println();
		
		sc.close();
	}
}
