# Step 1: Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType

local_library = "//C:/Users/15717/PycharmProjects/sparkProjects1/mysql-connector-j-8.3.0/mysql-connector-j-8.3.0.jar"

if __name__ == "__main__":
    # Step 2: Create a SparkSession
    spark = SparkSession \
        .builder \
        .appName("Python1") \
        .config("spark.jars", local_library) \
        .config("spark.driver.extraClassPath", local_library) \
        .config("spark.executor.extraClassPath", local_library) \
        .config("spark.sql.warehouse.dir", "file:///c:/tmp") \
        .enableHiveSupport() \
        .getOrCreate()

    # # Define the schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True)
    ])
    #
    # # Create the DataFrame with the specified schema
    # rdd = spark.sparkContext.parallelize([{'id': 12, 'name': "John", 'age': 30, 'department': "HR"}])
    # # write to spark
    # df = spark.createDataFrame(rdd)
    # # df = spark.createDataFrame(rdd, schema)
    # # rdd.toDF(schema).show()
    # df.show()

    # # Step 5: Write data to MySQL
    # df.write.jdbc("jdbc:mysql://localhost:3306/mysql", "employee",
    #               properties={"user": "root", "password": "123@Charles"})
    #
    # to set the logging level to "WARN"
    spark.sparkContext.setLogLevel("WARN")

    query = "(select * from employee where age > 10) temp"
    url = "jdbc:mysql://localhost:3306/mysql"
    properties = {"user": "root", "password": "123@Charles"}
    df_read = spark.read.options(url=url, table=query, properties=properties).schema(schema).load()

    df_read.show()
    # Step 6: Stop SparkSession
    spark.stop()
