package com.kpi;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;

import static com.kpi.LabHelper.*;

public class SQLSparkAPIExamples {

    private static final String CAST_SQL_SCHEMA_HONEY = "SELECT state," +
            "CAST(numcol as double) numcol, " +
            "CAST(yieldpercol as double) yieldpercol, " +
            "CAST(totalprod as double) totalprod, " +
            "CAST(stocks as double) stocks, " +
            "CAST(priceperlb as double) priceperlb, " +
            "CAST(prodvalue as double) prodvalue, " +
            "CAST(year as double) year " +
            "FROM HONEY";
    private static final String CAST_SQL_SCHEMA_HONEYRAW = "SELECT CAST(id as double) id," +
            "letter, " +
            "state, " +
            "header4, " +
            "header5, " +
            "header6, " +
            "header7, " +
            "header8 " +
            "FROM HONEYRAW";

    private static final String HONEY_TABLE_NAME = "HONEY";
    private static final String HONEY_RAW_TABLE_NAME = "HONEYRAW";

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder()
                .appName(APP_NAME)
                .master(MASTER)
                .config("spark.sql.warehouse.dir", "src/main/resources/")
                .getOrCreate();

        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");
        dataFrameReader.csv(HONEYPRODUCTION_CSV).createOrReplaceTempView(HONEY_TABLE_NAME);
        dataFrameReader.csv(HONEYRAW_1998TO2002_CSV).createOrReplaceTempView(HONEY_RAW_TABLE_NAME);

        sparkSession.sql(CAST_SQL_SCHEMA_HONEY);
        sparkSession.sql(CAST_SQL_SCHEMA_HONEYRAW);

        countTotalNumberOfRowsByState(sparkSession, AL_STATE);
        filterHoneyByNumCol(sparkSession);
        joinFunction(sparkSession);

    }

    private static void countTotalNumberOfRowsByState(final SparkSession sparkSession, final String state) {
        sparkSession.sql("SELECT COUNT(*) as state_count FROM HONEY WHERE state='" + state + "'").show();
    }

    private static void filterHoneyByNumCol(final SparkSession sparkSession) {
        sparkSession.sql("SELECT * FROM HONEY WHERE numcol > " + NUMCOL_MIN
                + " AND numcol < " + NUMCOL_MAX
                + " SORT BY numcol").show();
    }

    private static void joinFunction(final SparkSession sparkSession) {
//        sparkSession.sql("SELECT honey.state, SUM(honey.yieldpercol)," +
//                " (SELECT SUM(row.id) FROM HONEYRAW AS row WHERE row.state = honey.state)" +
//                " FROM HONEY AS honey" +
//                " GROUP BY honey.state")
//                .show();
    }
}
