import os
from pyspark.sql import SparkSession, functions as F

BUCKET = "user-m-gorchon-ece"
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "s3.seaweedfs.adm.adaltas.cloud")
spark = (
    SparkSession.builder.appName("fin-lab7")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{S3_ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

orders = spark.table("lab.silver.orders")
if orders.filter("order_id = 'new-order-1'").count() == 0:
    new_order = spark.createDataFrame(
        [("new-order-1", "bdd640fb-0667-4ad1-9c80-317fa3b1799d", "2020-05-01 12:00:00", 2, "bread")],
        "order_id string, user_id string, ordered_at string, quantity int, product string",
    ).withColumn("ordered_at", F.to_timestamp("ordered_at"))
    new_order.writeTo("lab.silver.orders").append()
    print("commande ajoutee")
else:
    print("commande deja presente")

spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at").show(truncate=False)
first_snapshot = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at LIMIT 1").first()[0]
print("avant :", spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {first_snapshot}").first()[0])
print("maintenant :", spark.table("lab.silver.orders").count())
spark.stop()