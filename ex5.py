import os
from pyspark.sql import SparkSession

BUCKET = "user-m-gorchon-ece"
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "s3.seaweedfs.adm.adaltas.cloud")
spark = (
    SparkSession.builder.appName("ex5")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{S3_ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", "http://polaris:8181/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", BUCKET)
    .config("spark.sql.catalog.polaris.credential", os.environ["POLARIS_DEV_CREDENTIAL"])
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark_result = spark.sql("""
    SELECT u.state,
           date_trunc('MONTH', o.ordered_at) AS month,
           count(*) AS orders,
           sum(o.quantity) AS units,
           count(DISTINCT o.user_id) AS buyers
    FROM polaris.silver.orders o
    JOIN polaris.silver.users u ON u.user_id = o.user_id
    GROUP BY 1, 2
""")
print("===== SPARK : lignes =", spark_result.count(), "/ commandes =", spark_result.agg({"orders": "sum"}).first()[0])
spark.stop()