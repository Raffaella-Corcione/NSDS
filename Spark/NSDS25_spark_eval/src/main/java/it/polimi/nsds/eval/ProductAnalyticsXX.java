package it.polimi.nsds.eval;

/*
 * Group XX
 * Members:
 */

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import static org.apache.spark.sql.functions.*;

public class ProductAnalyticsXX {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("local[8]")
                .appName("ProductAnalytics")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Load static datasets
        Dataset<Row> products = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("input/products.csv");
        // ProductID, Category, Price

        Dataset<Row> historicalPurchases = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("input/historicalPurchases.csv");
        // ProductID, Quantity

        // Streaming source: purchasesStream
        Dataset<Row> purchasesStream = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 5)
                .load();

        // Map the "value" counter to product IDs and quantities
        purchasesStream = purchasesStream
                .select(
                        col("timestamp"),
                        when(col("value").mod(10).lt(2), lit("P0582"))
                                .when(col("value").mod(10).lt(3), lit("P0112"))
                                .when(col("value").mod(10).lt(5), lit("P0536"))
                                .when(col("value").mod(10).equalTo(6), lit("P0742"))
                                .when(col("value").mod(10).equalTo(8), lit("P0027"))
                                .otherwise(lit("P00" + Math.random() % 100))
                                .alias("Product"),
                        (col("value").mod(5).plus(1)).alias("Quantity")       // 1–5 units
                );

        // ==========================================
        // Q1: Top 5 most purchased products.
        // compute the top 5 most purchased products (in
        //terms of total quantity) using the static dataset
        //historicalPurchases
        // ==========================================

        StreamingQuery topProducts = purchasesStream
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();
        // ==========================================
        // Q2: Revenue per (Category, TopProduct)
        // ==========================================

        Dataset<Row> revenue = null;

        // ==========================================
        // Q3: Streaming window for top products
        // ==========================================

        Dataset<Row> windowed = null;

        // ==========================================
        // Q4: Difference historical vs streaming
        // ==========================================

        Dataset<Row> diff = null;

    }
}
