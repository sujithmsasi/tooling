package com.cku

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions._
import scala.io.Source

object KafkaMessageValidator {

  // Function to load schema from a file
  def loadSchema(schemaFilePath: String): StructType = {
    val schemaString = Source.fromFile(schemaFilePath).getLines().mkString
    DataType.fromJson(schemaString).asInstanceOf[StructType]
  }

  // Function to validate JSON messages against the schema
  def validateMessages(
                        spark: SparkSession,
                        jsonPathOrKafkaSource: String,
                        schemaFilePath: String,
                        outputInvalidPath: String
                      ): DataFrame = {

    // Load the schema from the file
    val schema = loadSchema(schemaFilePath)

    // Read the JSON messages with the schema applied
    val validatedDF = spark.read.schema(schema)
      .json(jsonPathOrKafkaSource)
      .withColumn("is_valid", lit(true)) // Flag for valid records

    // Read the raw JSON messages (without schema) for comparison
    val rawMessages = spark.read.json(jsonPathOrKafkaSource)

    // Identify invalid records by excluding valid ones from the raw dataset
    val invalidRecords = rawMessages.except(validatedDF)
      .withColumn("is_valid", lit(false)) // Flag for invalid records

    // Log or save invalid records for further analysis
    invalidRecords.write
      .format("json")
      .mode("overwrite")
      .save(outputInvalidPath)

    // Union valid and invalid records to see the complete dataset with validity flags
    val finalDF = validatedDF.union(invalidRecords)
    finalDF
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("KafkaMessageValidatorApp")
      .master("local[*]")  // Runs Spark locally with as many threads as there are cores
      .getOrCreate()

    // Define paths
    val jsonPathOrKafkaSource = "path_to_your_json_file_or_kafka_source" // JSON or Kafka data source
    val schemaFilePath = "path_to_schema_file.txt" // Path to schema file
    val outputInvalidPath = "path_to_save_invalid_records" // Path for logging invalid records

    // Validate messages
    val finalDF = KafkaMessageValidator.validateMessages(
      spark,
      jsonPathOrKafkaSource,
      schemaFilePath,
      outputInvalidPath
    )

    // Show the results
    finalDF.show(false)
  }
}
