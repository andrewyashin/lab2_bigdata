package com.kpi;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.kpi.LabHelper.*;

public class DataFrameSparkAPIExamples {
    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder()
                .appName("Spark Titanic  Demo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "src/main/resources/")
                .getOrCreate();

        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");
        final Dataset<Row> honeyDataSet = dataFrameReader.csv(HONEYPRODUCTION_CSV);
        final Dataset<Row> honeyRawDataSet = dataFrameReader.csv(HONEYRAW_1998TO2002_CSV);

        countTotalNumberOfRowsByState(honeyDataSet, AL_STATE);
        filterHoneyByNumCol(honeyDataSet);
        joinFunction(honeyDataSet, honeyRawDataSet);
    }

    private static void countTotalNumberOfRowsByState(final Dataset dataset, final String state) {
        System.out.println(dataset.filter("state = \"" + state + "\"").count());
    }

    private static void filterHoneyByNumCol(final Dataset dataset) {
        dataset.filter("numcol > " + NUMCOL_MIN)
                .filter("numcol < " + NUMCOL_MAX)
                .sort("numcol").show();
    }

    private static void joinFunction(final Dataset honeyDataset, final Dataset honeyrawDataset) {
//        honeyDataset.groupBy(col("state")).agg(sum("yieldpercol"))
//                    .join(honeyrawDataset).;


    }
}
