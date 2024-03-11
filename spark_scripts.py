from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("Retail Analysis") \
    .getOrCreate()

# Baca tabel 'retail' dari database PostgreSQL menjadi DataFrame
retail_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/warehouse") \
    .option("dbtable", "retail") \
    .option("user", "user") \
    .option("password", "password") \
    .load()

# Lakukan analisis sederhana pada DataFrame 'retail_df'
# Contoh: menghitung jumlah data per kategori
analysis_result = retail_df.groupBy("category").count()

# Output hasil analisis ke dalam format CSV
analysis_result.write.csv("file:///home/Satriyo_Wisnu/dibimbing_spark_airflow/dags/output")

# Hentikan sesi Spark
spark.stop()