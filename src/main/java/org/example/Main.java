package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) throws TimeoutException {
        // Set Spark log level to ERROR
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("HospitalIncidentStreaming")
                .master("local[2]") // Use "yarn" for deployment on a YARN cluster
                .getOrCreate();

        // Set application log level to INFO
        Logger.getLogger("org.example").setLevel(Level.INFO);

        // Define the schema for streaming
        String schema = "Id INT, titre STRING, description STRING, service STRING, date STRING";

        // Read streaming data from the specified directory
        Dataset<Row> streamingDF = spark.readStream()
                .schema(schema)
                .csv("src/main/java/org/example");

        // Task 1: Display the number of incidents per service continuously
        Dataset<Row> incidentsByService = streamingDF.groupBy("service")
                .agg(count("Id").alias("incident_count"));

        StreamingQuery query1 = incidentsByService.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();

        // Task 2: Display the two years with the highest number of incidents continuously
        Dataset<Row> incidentsByYear = streamingDF.withColumn("year", substring(col("date"), 1, 4))
                .groupBy("year")
                .agg(count("Id").alias("incident_count"))
                .orderBy(col("incident_count").desc())
                .limit(2);

        StreamingQuery query2 = incidentsByYear.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();

        // Wait for the streaming to finish
        try {
            query1.awaitTermination();
            query2.awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }
}
