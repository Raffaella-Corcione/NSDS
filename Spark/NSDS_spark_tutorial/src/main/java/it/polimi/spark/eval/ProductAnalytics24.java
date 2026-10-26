package it.polimi.spark.eval;

/*
 * Group 24
 * Members: Capaccioni Chiara, Corcione Raffaella, Cusano Virgilio
 */

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.dstream.DStream;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

public class ProductAnalytics24 {

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
        // Q1: Top 5 most purchased products
        // ==========================================

        Dataset<Row> topProducts = historicalPurchases // assumption: no repeated product ids in historicalPurchases
                .withColumnRenamed("Quantity", "totalQuantity")
                .orderBy(desc("totalQuantity"))
                .limit(5);

        topProducts.cache();

        topProducts.show();

        // ==========================================
        // Q2: Revenue per (Category, TopProduct)
        // ==========================================

        final Dataset<Row> revenue = topProducts
                .join(products, topProducts.col("ProductID").equalTo(products.col("ProductID")))
                .drop(products.col("ProductID"))
                .withColumn("totalRevenue", col("Price").multiply(col("totalQuantity")))
                .select(col("Category"), col("ProductID"), col("totalRevenue"));

        revenue.show();

        // ==========================================
        // Q3: Streaming window for top products
        // ==========================================

        Dataset<Row> windowed = purchasesStream
                .groupBy(
                        window(col("timestamp"), "15 seconds", "5 seconds"),
                        col("Product")
                ).sum()
                .withColumnRenamed("sum(Quantity)", "streamQuantity")
                .join(topProducts, topProducts.col("ProductID").equalTo(purchasesStream.col("Product")));

        StreamingQuery q3 = windowed
                .select(col("window"), col("Product"), col("streamQuantity"))
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try{
            q3.awaitTermination();
        } catch (StreamingQueryException e){
            e.printStackTrace();
        }

        // ==========================================
        // Q4: Difference historical vs streaming
        // ==========================================

        StreamingQuery diff = windowed
                .withColumn("difference", col("totalQuantity").minus(col("streamQuantity")))
                .select(col("window"), col("Product"), col("totalQuantity"), col("streamQuantity"), col("difference"))
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try{
            diff.awaitTermination();
        } catch (StreamingQueryException e){
            e.printStackTrace();
        }
    }
}
