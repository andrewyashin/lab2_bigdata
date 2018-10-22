package com.kpi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static com.kpi.LabHelper.*;


public class RDDSparkAPIExamples {

    public static void main(String[] args) {
        SparkConf configuration = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster(MASTER);

        JavaSparkContext sparkContext = new JavaSparkContext(configuration);

        filterHoneyByNumCol(sparkContext);
        countTotalNumberOfRowsByState(sparkContext, AL_STATE);
        joinFunction(sparkContext);

    }

    private static void countTotalNumberOfRowsByState(final JavaSparkContext sparkContext, final String state) {
        JavaRDD<String> honeyProductions = sparkContext.textFile(HONEYPRODUCTION_WITHOUT_HEADER_CSV);
        long countALStateRows = honeyProductions.map(line -> line.split(CSV_SEPARATOR))
                .mapToPair(line -> new Tuple2<>(line[0], line))
                .filter(tuple -> tuple._1.contains(state))
                .count();
        System.out.println(" - Total number of rows of \"" + state + "\" state = " + countALStateRows);
    }

    private static void filterHoneyByNumCol(final JavaSparkContext sparkContext) {
        JavaRDD<String> honeyProductions = sparkContext.textFile(HONEYPRODUCTION_WITHOUT_HEADER_CSV);
        honeyProductions.map(line -> line.split(CSV_SEPARATOR))
                .mapToPair(line -> new Tuple2<>(Double.valueOf(line[1]), line))
                .filter(tuple -> tuple._1 < NUMCOL_MAX)
                .filter(tuple -> tuple._1 > NUMCOL_MIN)
                .sortByKey()
                .collect()
                .forEach(tuple -> System.out.println(String.join(CSV_SEPARATOR, tuple._2)));
    }

    private static void joinFunction(final JavaSparkContext sparkContext) {
        JavaRDD<String> honeyProductions = sparkContext.textFile(HONEYPRODUCTION_WITHOUT_HEADER_CSV);
        JavaRDD<String> statesNames = sparkContext.textFile(STATES_WITHOUT_HEADER_CSV);

        JavaPairRDD stateGrouped = honeyProductions.map(line -> line.split(CSV_SEPARATOR))
                .mapToPair(line -> new Tuple2<>(line[0], Double.valueOf(line[2])))
                .reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD statesGrouped = statesNames.map(line -> line.split(CSV_SEPARATOR))
                .mapToPair(line -> new Tuple2<>(line[1], line[0]));

        stateGrouped.join(statesGrouped)
                .collect()
                .forEach(System.out::println);

    }
}
