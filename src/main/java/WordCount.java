import java.util.Arrays;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;


public class WordCount {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Word Count");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> lines = sc.textFile(args[0]);

    JavaPairRDD<Integer, String> wordCounts = lines.flatMap(
        line -> Arrays.asList(line.toLowerCase().replaceAll("[^a-zA-Z0-9\\s]+", "").split("\\s+")).iterator())
        .filter(word -> isPalindrome(word))
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((x, y) -> x + y)
        .mapToPair(p -> p.swap()) //swap key and value, so that we can use sortByKey to rank the words that appear the most.
        .sortByKey(false);

    wordCounts.saveAsTextFile("wordcount-output-" + System.currentTimeMillis() + "/");

    sc.stop();
  }

  private static boolean isPalindrome(String word) {
    if (word.length() == 0) return false;
    if (word.length() == 1 && !word.equals("a") && !word.equals("i")) return false;

    int start = 0, end = word.length()-1;
    while (start < end) {
      if (word.charAt(start) != word.charAt(end)) return false;
      start++;
      end--;
    }
    return true;
  }

}
