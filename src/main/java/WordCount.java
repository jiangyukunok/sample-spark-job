import com.google.common.collect.Iterables;
import java.util.Arrays;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;


public class WordCount {
  private static final int ROWS_OF_PARTITIONS = 500;

  /**
   * Find all lists of anagrams
   */
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Word Count");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> lines = sc.textFile("file:///Users/kejiang/Developer/maildir/allen-p/*/*"); //("hdfs://localhost:9000/hdfs/*/*");

    JavaPairRDD<String, Iterable<String>> anagramGroups = lines.flatMap(
        line -> Arrays.asList(line.toLowerCase().replaceAll("[^a-zA-Z0-9\\s]+", "").split("\\s+")).iterator())
        .distinct()
        .mapToPair(word -> new Tuple2<>(getSortedForm(word), word))
        .groupByKey()
        .filter(group -> Iterables.size(group._2) > 1);
    int numOfFiles = (int) (anagramGroups.count()/ROWS_OF_PARTITIONS);
    anagramGroups = anagramGroups.coalesce(numOfFiles);

    anagramGroups.saveAsTextFile("file:///Users/kejiang/Developer/WordCount/anagram-" + System.currentTimeMillis() + "/");

    sc.stop();
  }

  private static String getSortedForm(String word) {
    char[] chars = word.toCharArray();
    Arrays.sort(chars);
    return String.valueOf(chars);
  }

  /**
   * Find all palindromes and sort by appearing times
   */
//  public static void main(String[] args) {
//    SparkConf conf = new SparkConf().setAppName("Word Count");
//    JavaSparkContext sc = new JavaSparkContext(conf);
//    JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/hdfs/*/*");
//
//    JavaPairRDD<Integer, String> palindromeCounts = lines.flatMap(
//        line -> Arrays.asList(line.toLowerCase().replaceAll("[^a-zA-Z0-9\\s]+", "").split("\\s+")).iterator())
//        .filter(word -> isPalindrome(word))
//        .mapToPair(word -> new Tuple2<>(word, 1))
//        .reduceByKey((x, y) -> x + y)
//        .mapToPair(p -> p.swap()) //swap key and value, so that we can use sortByKey to rank the words that appear the most.
//        .sortByKey(false);
//
//    int numOfFiles = (int) (palindromeCounts.count()/ROWS_OF_PARTITIONS);
//    palindromeCounts = palindromeCounts.coalesce(numOfFiles);
//    palindromeCounts.saveAsTextFile("/spark/palindrome-" + System.currentTimeMillis() + "/");
//
//    sc.stop();
//  }
//
//  private static boolean isPalindrome(String word) {
//    if (word.length() == 0) return false;
//    if (word.length() == 1 && !word.equals("a") && !word.equals("i")) return false;
//
//    int start = 0, end = word.length()-1;
//    while (start < end) {
//      if (word.charAt(start) != word.charAt(end)) return false;
//      start++;
//      end--;
//    }
//    return true;
//  }

}
