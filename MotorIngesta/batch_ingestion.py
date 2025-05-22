from pyspark.sql.functions import current_timestamp, input_file_name, replace,lit

class LandingStreamReader:

    def __init__(self, builder):
        self.datasource = builder.datasource
        self.dataset = builder.dataset
        self.landing_path = builder.landing_path
        self.raw_path = builder.raw_path
        self.bronze_path = builder.bronze_path
        self.format = builder.format
        self.dataset_landing_path = f'{self.landing_path}/{self.datasource}/{self.dataset}'
        self.dataset_bronze_schema_location = f'{self.bronze_path}/{self.datasource}/{self.dataset}_schema'
        self.spark = builder.spark
        #dbutils.fs.mkdirs(self.dataset_bronze_schema_location)
    
    def __str__(self):
        return (f"LandingStreamReader(datasource='{self.datasource}',dataset='{self.dataset}')")
        
    def add_metadata_columns(self,df):
      data_cols = df.columns
      
      metadata_cols = ['_ingested_at','_ingested_filename']

      df = (df.withColumn("_ingested_at",current_timestamp())
              .withColumn("_ingested_filename",replace(input_file_name(),lit(self.landing_path),lit(self.raw_path)))
      ) 
      
      #reordernamos columnas
      return df.select(metadata_cols + data_cols)  
    
    def read_json(self):
      return (self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", self.dataset_bronze_schema_location)
            .load(self.dataset_landing_path)
      )
    
    def read(self):
      df = None

      if (self.format == "json"):
        df = self.read_json()
      else:
        raise Exception(f"Format {self.format} not supported")

      if df:
        df = df.transform(self.add_metadata_columns)
      return df
    
    class Builder:
        def __init__(self):
            self.datasource = None
            self.dataset = None
            self.landing_path = None
            self.raw_path = None
            self.bronze_path = None
            self.format = None
            self.spark = None

        def set_datasource(self, datasource):
            self.datasource = datasource
            return self
        
        def set_dataset(self, dataset):
            self.dataset = dataset
            return self
        
        def set_landing_path(self, landing_path):
            self.landing_path = landing_path
            return self
        
        def set_raw_path(self, raw_path):
            self.raw_path = raw_path
            return self
        
        def set_bronze_path(self, bronze_path):
            self.bronze_path = bronze_path
            return self
          
        def set_format(self, format):
            self.format = format
            return self
        
        def set_spark(self, spark):
            self.spark = spark
            return self
        
        def build(self):
            return LandingStreamReader(self)
        
class BronzeStreamWriter:
    def __init__(self, builder):
        self.datasource = builder.datasource
        self.dataset = builder.dataset
        self.landing_path = builder.landing_path
        self.raw_path = builder.raw_path
        self.bronze_path = builder.bronze_path
        self.dataset_landing_path = f"{self.landing_path}/{self.datasource}/{self.dataset}"
        self.dataset_raw_path =  f"{self.raw_path}/{self.datasource}/{self.dataset}"
        self.dataset_bronze_path = f"{self.bronze_path}/{self.datasource}/{self.dataset}"
        self.dataset_checkpoint_location = f'{self.dataset_bronze_path}_checkpoint'
        self.table = f'hive_metastore.bronze.{self.datasource}_{self.dataset}'
        self.query_name = f"bronze-{self.datasource}-{self.dataset}"
        self.spark = builder.spark
        #dbutils.fs.mkdirs(self.dataset_raw_path)
        #dbutils.fs.mkdirs(self.dataset_bronze_path)
        #dbutils.fs.mkdirs(self.dataset_checkpoint_location)

    def __str__(self):
        return (f"BronzeStreamWriter(datasource='{self.datasource}',dataset='{self.dataset}')")
         
    def archive_raw_files(self,df):
      if "_ingested_filename" in df.columns:
        files = [row["_ingested_filename"] for row in df.select("_ingested_filename").distinct().collect()]
        for file in files:
          if file:
              file_landing_path = file.replace(self.dataset_raw_path,self.dataset_landing_path)
              #dbutils.fs.mkdirs(file[0:file.rfind('/')+1])
              #dbutils.fs.mv(file_landing_path,file)
    
    def write_data(self,df):
      self.spark.sql( 'CREATE DATABASE IF NOT EXISTS hive_metastore.bronze') 
      self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} USING DELTA LOCATION '{self.dataset_bronze_path}' ") 
      (df.write
          .format("delta")  
          .mode("append")
          .option("mergeSchema", "true")
          .option("path", self.dataset_bronze_path)
          .saveAsTable(self.table)
      )
        
    def append_2_bronze(self,batch_df, batch_id):
      batch_df.persist()
      self.write_data(batch_df)
      self.archive_raw_files(batch_df)
      batch_df.unpersist()
      

    class Builder:
        def __init__(self):
            self.datasource = None
            self.dataset = None
            self.landing_path = None
            self.raw_path = None
            self.bronze_path = None
            self.spark = None
        
        def set_datasource(self, datasource):
            self.datasource = datasource
            return self
        
        def set_dataset(self, dataset):
            self.dataset = dataset
            return self
        
        def set_landing_path(self, landing_path):
            self.landing_path = landing_path
            return self
        
        def set_raw_path(self, raw_path):
            self.raw_path = raw_path
            return self
        
        def set_bronze_path(self, bronze_path):
            self.bronze_path = bronze_path
            return self
        
        def set_spark(self, spark):
            self.spark = spark
            return self
        
        def build(self):
            return BronzeStreamWriter(self)
        

def batch_ingestion(datasource, dataset, landing_path, raw_path, bronze_path, format, spark):

    reader = (LandingStreamReader.Builder()          
    .set_datasource(datasource)
    .set_dataset(dataset)
    .set_landing_path(landing_path)
    .set_raw_path(raw_path)
    .set_bronze_path(bronze_path)
    .set_format(format)
    .set_spark(spark)
    .build()
    )
    
    writer = (BronzeStreamWriter.Builder()
    .set_datasource(datasource)
    .set_dataset(dataset)
    .set_landing_path(landing_path)
    .set_raw_path(raw_path)
    .set_bronze_path(bronze_path)
    .set_spark(spark)
    .build()
    )

    print(reader)
    print(writer)

    (reader
    .read()
    .writeStream
    .foreachBatch(writer.append_2_bronze)
    #.trigger(availableNow=True)
    .trigger(processingTime="3600 seconds") # modo continuo
    .option("checkpointLocation", writer.dataset_checkpoint_location)
    .queryName(writer.query_name)
    .start()
    )