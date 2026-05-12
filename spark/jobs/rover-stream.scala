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

val output = cleanTelemetry
  .withColumn("eventDate", to_date(col("eventTime")))

val query = output
  .writeStream
  .format("parquet")
  .outputMode("append")
  .option("path", "/opt/spark/lake/rover-telemetry-clean")
  .option("checkpointLocation", "/tmp/spark-checkpoints/rover-stream-parquet")
  .partitionBy("eventDate")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()

println(s"Started Spark parquet sink from Kafka topic: $inputTopic")
println("Writing to /opt/spark/lake/rover-telemetry-clean")

query.awaitTermination()