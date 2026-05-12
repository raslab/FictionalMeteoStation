import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

spark.sparkContext.setLogLevel("WARN")

val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
val inputTopic = sys.env.getOrElse("KAFKA_TOPIC", "rover.telemetry.raw")

val telemetrySchema = new StructType()
  .add("eventId", StringType)
  .add("roverId", StringType)
  .add("timestampUtc", StringType)
  .add("lat", DoubleType)
  .add("lon", DoubleType)
  .add("airQualityIndex", IntegerType)
  .add("airQualityRaw", DoubleType)
  .add("batteryPercent", DoubleType)

val rawKafka = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBootstrapServers)
  .option("subscribe", inputTopic)
  .option("startingOffsets", "latest")
  .load()

val parsedTelemetry = rawKafka
  .select(
    col("key").cast("string").as("kafkaKey"),
    col("value").cast("string").as("json"),
    col("timestamp").as("kafkaTimestamp")
  )
  .select(
    col("kafkaKey"),
    col("kafkaTimestamp"),
    from_json(col("json"), telemetrySchema).as("data"),
    col("json")
  )
  .select(
    col("kafkaKey"),
    col("kafkaTimestamp"),
    col("data.eventId").as("eventId"),
    col("data.roverId").as("roverId"),
    to_timestamp(col("data.timestampUtc")).as("eventTime"),
    col("data.lat").as("latitude"),
    col("data.lon").as("longitude"),
    col("data.airQualityIndex").as("airQualityIndex"),
    col("data.airQualityRaw").as("airQualityRaw"),
    col("data.batteryPercent").as("batteryPercent"),
    col("json")
  )
  .where(col("eventId").isNotNull)

val cleanTelemetry = parsedTelemetry
  .withWatermark("eventTime", "30 seconds")
  .dropDuplicates("eventId")

val parquetOutput = cleanTelemetry
  .withColumn("eventDate", to_date(col("eventTime")))

val parquetQuery = parquetOutput
  .writeStream
  .format("parquet")
  .outputMode("append")
  .option("path", "/opt/spark/lake/rover-telemetry-clean")
  .option("checkpointLocation", "/tmp/spark-checkpoints/rover-stream-parquet")
  .partitionBy("eventDate")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()

val cleanKafkaOutput = cleanTelemetry
 .select(
   col("roverId").as("key"),
   to_json(struct(
     col("eventId"),
     col("roverId"),
     col("eventTime"),
     col("latitude"),
     col("longitude"),
     col("airQualityIndex"),
     col("airQualityRaw"),
     col("batteryPercent")
   )).as("value")
 )
 .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

val cleanKafkaQuery = cleanKafkaOutput
 .writeStream
 .format("kafka")
 .option("kafka.bootstrap.servers", kafkaBootstrapServers)
 .option("topic", "rover.telemetry.clean")
 .option("checkpointLocation", "/tmp/spark-checkpoints/rover-telemetry-clean-kafka")
 .outputMode("append")
 .trigger(Trigger.ProcessingTime("5 seconds"))
 .start()

val alerts = cleanTelemetry
 .where(col("airQualityIndex") >= 100 || col("batteryPercent") <= 20)
 .withColumn(
   "alertType",
   when(col("airQualityIndex") >= 100, lit("HIGH_AIR_POLLUTION"))
     .otherwise(lit("LOW_BATTERY"))
 )
 .withColumn("alertId", concat(col("eventId"), lit(":"), col("alertType")))

val alertsKafkaOutput = alerts
 .select(
   col("roverId").as("key"),
   to_json(struct(
     col("alertId"),
     col("alertType"),
     col("eventId"),
     col("roverId"),
     col("eventTime"),
     col("latitude"),
     col("longitude"),
     col("airQualityIndex"),
     col("airQualityRaw"),
     col("batteryPercent")
   )).as("value")
 )
 .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

val alertsKafkaQuery = alertsKafkaOutput
 .writeStream
 .format("kafka")
 .option("kafka.bootstrap.servers", kafkaBootstrapServers)
 .option("topic", "rover.alerts")
 .option("checkpointLocation", "/tmp/spark-checkpoints/rover-alerts-kafka")
 .outputMode("append")
 .trigger(Trigger.ProcessingTime("5 seconds"))
 .start()

println(s"Started Spark stream from Kafka topic: $inputTopic")
println("Writing Parquet history to /opt/spark/lake/rover-telemetry-clean")
println("Writing clean events to rover.telemetry.clean")
println("Writing alerts to rover.alerts")

spark.streams.awaitAnyTermination()
