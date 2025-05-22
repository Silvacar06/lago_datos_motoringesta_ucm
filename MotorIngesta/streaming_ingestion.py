import pyspark.sql.functions as F
from pyspark.sql.avro.functions import from_avro

def read_config():
  config = {}
  with open("/dbfs/FileStore/client_properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def streaming_ingestion(topic, dataset_bronze_path, dataset_bronze_checkpoint_path, table_name, spark, schema_registry_client):
    
    conf = read_config()

    kafka_options = {
        "kafka.bootstrap.servers": conf["bootstrap.servers"],
        "kafka.security.protocol": conf["security.protocol"],
        "kafka.sasl.mechanism":   conf["sasl.mechanisms"],
        "kafka.sasl.jaas.config":
                f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """,
        "subscribe":topic,
        "includeHeaders" : "true",
        "startingOffsets": "earliest"
    }

    # creamos un streaming dataframe
    df = (spark
        .readStream
        .format("kafka") 
        .options(**kafka_options)
        .load()
    )

    #renombramos columnas
    columns = [F.col(column).alias(f'_{column}') for column in df.columns]
    df = df.select(*columns)

    value_subject = f"{topic}-value"
    value_schema = schema_registry_client.get_latest_version(value_subject).schema.schema_str 

    print(value_subject)
    print(value_schema)

    df=(df
        #.withColumn("key",F.col("_key").cast("string"))
        .withColumn("value",from_avro(F.expr("substring(_value,6,length(_value)-5)"),value_schema))
        .withColumn("_ingested_at",F.current_timestamp())  #metadata
        .select("*","value.*")
        .drop("value")
    )

    (df
        .writeStream
        .trigger(availableNow=True)
        #.trigger(processingTime="60 seconds") # modo continuo
        .format("delta")
        .outputMode("append")
        .option("path", dataset_bronze_path)
        .option("mergeSchema", "true")
        .option("checkpointLocation", dataset_bronze_checkpoint_path)
        .table(table_name)
    )