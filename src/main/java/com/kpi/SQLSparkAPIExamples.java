package com.kpi;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.kpi.LabHelper.*;

public class SQLSparkAPIExamples {

    private static final String CAST_SQL_SCHEMA = "SELECT state," +
            "CAST(numcol as double) numcol, " +
            "CAST(yieldpercol as double) yieldpercol, " +
            "CAST(totalprod as double) totalprod, " +
            "CAST(stocks as double) stocks, " +
            "CAST(priceperlb as double) priceperlb, " +
            "CAST(prodvalue as double) prodvalue, " +
            "CAST(year as double) year " +
            "FROM HONEY";
    private static final String TABLE_NAME = "HONEY";

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder()
                .appName(APP_NAME)
                .master(MASTER)
                .config("spark.sql.warehouse.dir", "src/main/resources/")
                .getOrCreate();

        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");
        final Dataset<Row> csvDataFrame = dataFrameReader.csv(HONEYPRODUCTION_CSV);

        csvDataFrame.createOrReplaceTempView(TABLE_NAME);
        final Dataset<Row> honeyRawData = sparkSession.sql(CAST_SQL_SCHEMA);

        countTotalNumberOfRowsByState(sparkSession, AL_STATE);
        filterHoneyByNumCol(sparkSession);

    }

    private static void countTotalNumberOfRowsByState(final SparkSession sparkSession, final String state) {
        sparkSession.sql("SELECT COUNT(*) as state_count FROM HONEY WHERE state='" + state + "'").show();
    }

    private static void filterHoneyByNumCol(final SparkSession sparkSession) {
        sparkSession.sql("SELECT * FROM HONEY WHERE numcol > " + NUMCOL_MIN
                + " AND numcol < " + NUMCOL_MAX
                + " SORT BY numcol").show();
    }
}
