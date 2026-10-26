package it.polimi.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;
import static scala.Predef.Map;

/*
 * Group number: XX
 *
 * Group members
 *  - Student 1
 *  - Student 2
 *  - Student 3
 */

public class SparkGroupXX {
    private static final int numCourses = 3000;

    public static void main(String[] args) throws TimeoutException, IOException {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("SparkEval")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> profsFields = new ArrayList<>();
        profsFields.add(DataTypes.createStructField("prof_name", DataTypes.StringType, false));
        profsFields.add(DataTypes.createStructField("course_name", DataTypes.StringType, false));
        final StructType profsSchema = DataTypes.createStructType(profsFields);

        final List<StructField> coursesFields = new ArrayList<>();
        coursesFields.add(DataTypes.createStructField("course_name", DataTypes.StringType, false));
        coursesFields.add(DataTypes.createStructField("course_hours", DataTypes.IntegerType, false));
        coursesFields.add(DataTypes.createStructField("course_students", DataTypes.IntegerType, false));
        final StructType coursesSchema = DataTypes.createStructType(coursesFields);

        final List<StructField> videosFields = new ArrayList<>();
        videosFields.add(DataTypes.createStructField("video_id", DataTypes.IntegerType, false));
        videosFields.add(DataTypes.createStructField("video_duration", DataTypes.IntegerType, false));
        videosFields.add(DataTypes.createStructField("course_name", DataTypes.StringType, false));
        final StructType videosSchema = DataTypes.createStructType(videosFields);

        // Professors: prof_name, course_name
        final Dataset<Row> profs = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(profsSchema)
                .csv(filePath + "files/profs.csv");

        // Courses: course_name, course_hours, course_students
        final Dataset<Row> courses = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(coursesSchema)
                .csv(filePath + "files/courses.csv");

        // Videos: video_id, video_duration, course_name
        final Dataset<Row> videos = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(videosSchema)
                .csv(filePath + "files/videos.csv");

        // Visualizations: value, timestamp
        // value represents the video id
        final Dataset<Row> visualizations = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 10)
                .load()
                .withColumn("value", col("value").mod(numCourses));

        courses.cache();
        videos.cache();

        /*
         * Query Q1. Compute the total number of lecture hours per prof
         */

        final Dataset<Row> q1 = profs
                .join(courses, profs.col("course_name").equalTo(courses.col("course_name")))
                .drop(profs.col("course_name"))
                .groupBy(col("prof_name"))
                .sum("course_hours")
                .select(col("prof_name"), col("sum(course_hours)"));

        q1.show();

        /*
         * Query Q2. For each course, compute the total duration of all the visualizations of videos of that course,
         * computed over a minute, updated every 10 seconds
         */

        final StreamingQuery q2 = visualizations
                .join(videos, visualizations.col("value").equalTo(videos.col("video_id")))
                .drop(visualizations.col("value"))
                .groupBy(
                        window(col("timestamp"), "60 seconds", "10 seconds"),
                        col("course_name")
                ).sum("video_duration")
                .select(col("course_name"), col("sum(video_duration)"))
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        /*
         * Query Q3. For each video, compute the total number of visualizations of that video
         * with respect to the number of students in the course in which the video is used.
         */

        // group the streaming data by value and count -> get number of visualizations for each video and video_id
        final Dataset<Row> visualizationsByVideo = visualizations
                .groupBy(col("value"))
                .count();

        // join visualizationsByVideo and videos -> get course_name and number of views for each video of that course
        final Dataset<Row> visualizationsByCourse = visualizationsByVideo
                .join(videos, videos.col("video_id").equalTo(visualizationsByVideo.col("value")))
                .drop(videos.col("video_duration"), visualizationsByVideo.col("value"));

        final Dataset<Row> visualizationsByStudents = visualizationsByCourse
                .join(courses, courses.col("course_name").equalTo(visualizationsByCourse.col("course_name")))
                .drop(courses.col("course_hours"), courses.col("course_name"))
                .withColumn("visualizationsByStudents", col("count").divide(courses.col("course_students")));

        final StreamingQuery q3 = visualizationsByStudents
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            q2.awaitTermination();
            q3.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}