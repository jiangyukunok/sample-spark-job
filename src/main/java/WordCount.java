import java.util.Arrays;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;


public class WordCount {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Word Count");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> lines = sc.textFile("gs://bessie_cloud_bucket-1/rose.txt");

    JavaPairRDD wordCounts = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
        .mapToPair(word -> new Tuple2(word, 1))
        .reduceByKey((x, y) -> (int)x + (int)y);
    wordCounts.saveAsTextFile("gs://bessie_cloud_bucket-1/wordcount-output/");

    sc.stop();
  }
}
