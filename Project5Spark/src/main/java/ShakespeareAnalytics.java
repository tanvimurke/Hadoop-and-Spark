/**
This performs text analysis on a file containing the works of Shakespeare using Apache Spark's Java
The program counts the total number of lines, words, symbols, distinct symbols, and distinct letters in the file.
Additionally, it prompts the user to enter a search term and displays all lines in the file that contain the
search term.
 Author: Tanvi Murke
 Andrew ID: tmurke
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import java.util.Arrays;
import java.util.Scanner;

public class ShakespeareAnalytics {

    private static void analytics(String fileName) {
        //spark configuration
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("ShakespeareAnalytics");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile(fileName);

        // Task 0
        //Counts the total number of lines in the file
        long numLines = inputFile.count();

        System.out.println("Task 0");
        System.out.println("Number of lines in the file: " + numLines);

        // Task 1
        // Counts the total number of words in the file
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split("[^a-zA-Z]+")));

        Function<String, Boolean> filter = k -> ( !k.isEmpty());

        long numWords = wordsFromFile.filter(filter).count();
        System.out.println("Task 1");
        System.out.println("Number of words in the file: " + numWords);

        // Task 2
        //Counts the total number of words in the file and the number of distinct words.
        long numDistinctWords = wordsFromFile.filter(filter).distinct().count();
        System.out.println("Task 2");
        System.out.println("Number of distinct words in the file: " + numDistinctWords);

        // Task 3
        //Counts the total number of symbols in the file.
        JavaRDD<String> symbolsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split("")));
        long numSymbols = symbolsFromFile.count();
        System.out.println("Task 3");
        System.out.println("Number of symbols in the file: " + numSymbols);

        // Task 4
        //Counts the number of distinct symbols in the file.
        long numDistinctSymbols = symbolsFromFile.distinct().count();
        System.out.println("Task 4");
        System.out.println("Number of distinct symbols in the file: " + numDistinctSymbols);

        // Task 5
        //Counts the number of distinct letters (case-insensitive) in the file.
        JavaRDD<String> lettersFromFile = symbolsFromFile.filter(s -> s.matches("[a-zA-Z]"));
        long numDistinctLetters = lettersFromFile.distinct().count();
        System.out.println("Task 5");
        System.out.println("Number of distinct letters in the file: " + numDistinctLetters);

        // Task 6
        //Prompts the user to enter a search term and displays all lines in the file that contain the search term.
        Scanner scanner = new Scanner(System.in);
        System.out.println("Task 6");
        System.out.print("Enter a word to search for: ");
        String searchTerm = scanner.next();
        JavaRDD<String> matchingLines = inputFile.filter(line -> line.contains(searchTerm));
        System.out.println("Matching lines:");
        matchingLines.foreach(line -> System.out.println(line));

    }

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        analytics(args[0]);
    }
}
