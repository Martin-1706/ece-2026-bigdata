import os
from pyspark.sql import SparkSession, functions as F

BUCKET = "user-m-gorchon-ece"
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "s3.seaweedfs.adm.adaltas.cloud")
spark = (
    SparkSession.builder.appName("lab8")
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
sql = spark.sql

print("===== Bloc 3 : la table orders")
sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders").show()

print("===== Bloc 5 : tables de metadonnees")
sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at").show(truncate=False)
sql("SELECT file_path, record_count FROM lab.silver.orders.files").show(truncate=False)

print("===== Bloc 6 : un nouveau snapshot (+3 commandes)")
nb_snap = sql("SELECT count(*) FROM lab.silver.orders.snapshots WHERE operation = 'append'").first()[0]
if nb_snap < 2:
    orders = spark.table("lab.silver.orders")
    orders.limit(3).withColumn("order_id", F.expr("uuid()")).writeTo("lab.silver.orders").append()
    print("3 commandes ajoutees")
else:
    print("deja fait")

print("===== Bloc 7 : time travel")
first = sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at LIMIT 1").first()[0]
print("premiere version :", sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {first}").first()[0])
print("maintenant       :", spark.table("lab.silver.orders").count())

print("===== Bloc 8 : evolution de schema (colonne source)")
if "source" not in spark.table("lab.silver.orders").columns:
    sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
    sql("INSERT INTO lab.silver.orders SELECT order_id, user_id, ordered_at, quantity, product, 'spark' FROM lab.silver.orders LIMIT 1")
    print("colonne source ajoutee")
else:
    print("deja fait")
sql("SELECT source, count(*) FROM lab.silver.orders GROUP BY source").show()

print("===== Exercices 1-2 : users, ajout de 2 utilisateurs")
if sql("SELECT count(*) FROM lab.silver.users.snapshots WHERE operation = 'append'").first()[0] == 0:
    users = spark.table("lab.silver.users")
    (users.limit(2)
        .withColumn("user_id", F.expr("uuid()"))
        .withColumn("username", F.concat(F.col("username"), F.lit("_new")))
        .writeTo("lab.silver.users").append())
    print("2 utilisateurs ajoutes")
else:
    print("deja fait")
sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.users.snapshots ORDER BY committed_at").show(truncate=False)

print("===== Exercice 3 : avant / apres")
before = sql("SELECT snapshot_id FROM lab.silver.users.snapshots ORDER BY committed_at LIMIT 1").first()[0]
print("avant :", sql(f"SELECT count(*) FROM lab.silver.users VERSION AS OF {before}").first()[0])
sql(f"SELECT username FROM lab.silver.users WHERE user_id NOT IN (SELECT user_id FROM lab.silver.users VERSION AS OF {before})").show()

print("===== Exercice 4 : colonne country")
if "country" not in spark.table("lab.silver.users").columns:
    sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
    sql("INSERT INTO lab.silver.users SELECT user_id, username, name, sex, email, birthdate, street, city, state, zip_code, 'US' FROM lab.silver.users LIMIT 1")
    print("colonne country ajoutee")
else:
    print("deja fait")
sql("SELECT country, count(*) FROM lab.silver.users GROUP BY country").show()

print("===== FIN : orders =", spark.table("lab.silver.orders").count(), "/ users =", spark.table("lab.silver.users").count())
spark.stop()