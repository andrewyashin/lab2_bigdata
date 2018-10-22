package com.kpi;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;

import static com.kpi.LabHelper.AL_STATE;
import static com.kpi.LabHelper.APP_NAME;
import static com.kpi.LabHelper.HONEYPRODUCTION_CSV;
import static com.kpi.LabHelper.MASTER;
import static com.kpi.LabHelper.NUMCOL_MAX;
import static com.kpi.LabHelper.NUMCOL_MIN;
import static com.kpi.LabHelper.STATES_CSV;

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

    private static final String HONEY_TABLE_NAME = "HONEY";
    private static final String STATES_NAMES_TABLE_NAME = "STATES_NAMES";

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder()
                .appName(APP_NAME)
                .master(MASTER)
                .config("spark.sql.warehouse.dir", "src/main/resources/")
                .getOrCreate();

        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");
        dataFrameReader.csv(HONEYPRODUCTION_CSV).createOrReplaceTempView(HONEY_TABLE_NAME);
        dataFrameReader.csv(STATES_CSV).createOrReplaceTempView(STATES_NAMES_TABLE_NAME);

        sparkSession.sql(CAST_SQL_SCHEMA_HONEY);

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
        sparkSession.sql("SELECT SUM(honey.yieldpercol) AS sum_yieldpercol, " +
                "honey.state, " +
                "state.fullName " +
                "FROM HONEY AS honey " +
                "INNER JOIN STATES_NAMES AS state " +
                "ON honey.state = state.state " +
                "GROUP BY honey.state, state.fullName").show();
    }
}

