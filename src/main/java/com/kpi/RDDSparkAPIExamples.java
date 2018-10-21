package com.kpi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class RDDSparkAPIExamples {
    private static final String HONEYPRODUCTION_CSV = "src/main/resources/honeyproduction.csv";
    private static final String APP_NAME = "LAB2";
    private static final String MASTER = "local[*]";
    private static final double NUMCOL_MAX = 20000d;
    private static final String CSV_SEPARATOR = ",";
    private static final String AL_STATE = "AL";

    public static void main(String[] args) {
        SparkConf configuration = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster(MASTER);

        JavaSparkContext sparkContext = new JavaSparkContext(configuration);

        filterHoneyByNumCol(sparkContext);
        countTotalNumberOfRowsByState(sparkContext, AL_STATE);

    }

    private static void countTotalNumberOfRowsByState(final JavaSparkContext sparkContext, final String state) {
        JavaRDD<String> honeyProductions = sparkContext.textFile(HONEYPRODUCTION_CSV);
        long countALStateRows = honeyProductions.map(line -> line.split(CSV_SEPARATOR))
                .mapToPair(line -> new Tuple2<>(line[0], line))
                .filter(tuple -> tuple._1.contains(state))
                .count();
        System.out.println(" - Total number of rows of \"" + state + "\" state = " + countALStateRows);
    }

    private static void filterHoneyByNumCol(final JavaSparkContext sparkContext) {
        JavaRDD<String> honeyProductions = sparkContext.textFile(HONEYPRODUCTION_CSV);
        honeyProductions.map(line -> line.split(CSV_SEPARATOR))
                .mapToPair(line -> new Tuple2<>(Double.valueOf(line[1]), line))
                .filter(tuple -> tuple._1 < NUMCOL_MAX)
                .sortByKey()
                .collect()
                .forEach(tuple -> System.out.println(String.join(CSV_SEPARATOR, tuple._2)));
    }
}
