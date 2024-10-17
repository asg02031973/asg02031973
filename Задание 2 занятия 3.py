from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from datetime import datetime
import random

# Создаем объект SparkSession
spark = SparkSession.builder \
    .appName("Интернет-магазин") \
    .getOrCreate()
# Создаем схему
schema = StructType([
    StructField("Дата", StringType(), False),
    StructField("User_ID", IntegerType(), False),
    StructField("Продукт", StringType(), True),
    StructField("Количество", IntegerType(), True),
    StructField("Цена", IntegerType(), True)
])
# Создаем DataFrame
data = [(f"2024-{random.randint(3, 12)}-{random.randint(1, 30)}", i, f"Продукт_{i}",
         random.randint(1, 100), random.randint(100, 100000)) for i in range(1, 1000)]
df = spark.createDataFrame(data, schema)
# Показываем содержимое DataFrame
df.printSchema()
df.show()
#  Сохраняем DataFrame в файл csv
df.repartition(1).write.csv("project-1/results")
spark.stop()


