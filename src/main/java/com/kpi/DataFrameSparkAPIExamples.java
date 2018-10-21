package com.kpi;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

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
        final Dataset<Row> csvDataFrame = dataFrameReader.csv(HONEYPRODUCTION_CSV);

        csvDataFrame.printSchema();
        csvDataFrame.show();

    }

    private static class HoneyProduction implements Serializable {
        private String state;
        private Double numcol;
        private Double yieldpercol;
        private Double totalprod;
        private Double stocks;
        private Double priceperlb;
        private Double prodvalue;
        private Double year;
    }
}
