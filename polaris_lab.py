import os
import sys
from pyspark.sql import SparkSession

STEP = sys.argv[1]
PRINCIPAL = {"dev1": "dev", "analyst": "analyst", "dev2": "dev", "silver_analyst": "silver_analyst", "dev3": "dev"}[STEP]
BUCKET = os.environ["LAB_BUCKET_NAME"]
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "s3.seaweedfs.adm.adaltas.cloud")

spark = (
    SparkSession.builder.appName(f"lab-polaris-{STEP}")
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
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{os.environ['POLARIS_URI']}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", os.environ["POLARIS_CATALOG"])
    .config("spark.sql.catalog.polaris.credential", os.environ[f"POLARIS_{PRINCIPAL.upper()}_CREDENTIAL"])
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
sql = spark.sql
print(f"===== Etape {STEP}, Polaris principal : {PRINCIPAL}")


def exists(table):
    try:
        spark.table(table).limit(0).collect()
        return True
    except Exception:
        return False


def must_fail(label, query):
    try:
        sql(query).collect()
        print(f"{label} : AUTORISE (pas normal !)")
    except Exception as e:
        msg = str(e).splitlines()[0][:150]
        print(f"{label} : REFUSE (c'est attendu) -> {msg}")


if STEP == "dev1":
    print("===== B3 : les catalogues")
    sql("SHOW NAMESPACES IN lab").show()
    sql("SHOW NAMESPACES IN polaris").show()
    print("===== B4 : enregistrer orders dans Polaris")
    sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    if not exists("polaris.silver.orders"):
        version = spark.read.text(f"s3a://{BUCKET}/warehouse/silver/orders/metadata/version-hint.text").first()[0].strip()
        metadata_file = f"s3a://{BUCKET}/warehouse/silver/orders/metadata/v{version}.metadata.json"
        print(metadata_file)
        sql(f"CALL polaris.system.register_table(table => 'silver.orders', metadata_file => '{metadata_file}')").show()
    else:
        print("deja fait")
    print("orders polaris :", spark.table("polaris.silver.orders").count(), "/ orders lab :", spark.table("lab.silver.orders").count())
    print("===== B5 : creer users dans Polaris")
    sql("SHOW TABLES IN polaris.silver").show()
    sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)
    if not exists("polaris.silver.users"):
        stg_users = spark.table("lab.silver.users").drop("country").dropDuplicates(["user_id"])
        stg_users.writeTo("polaris.silver.users").createOrReplace()
    else:
        print("deja fait")
    print("users polaris :", spark.table("polaris.silver.users").count())
    print("===== B6 : ajouter la colonne country")
    if "country" not in spark.table("polaris.silver.users").columns:
        sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
    sql("DESCRIBE polaris.silver.users").show()

elif STEP == "analyst":
    print("===== B7 : lecture en analyst")
    sql("SHOW TABLES IN polaris.silver").show()
    sql("SELECT product, count(*) FROM polaris.silver.orders GROUP BY product").show()
    must_fail("INSERT dans orders", "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    must_fail("ALTER TABLE users", "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")

elif STEP == "dev2":
    print("===== B7 fin : l'INSERT refuse n'a rien cree")
    sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)
    print("===== Exercice 1 : supprimer country")
    sql("SELECT snapshot_id, operation FROM polaris.silver.users.snapshots").show()
    if "country" in spark.table("polaris.silver.users").columns:
        sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
    sql("DESCRIBE polaris.silver.users").show()
    sql("SELECT snapshot_id, operation FROM polaris.silver.users.snapshots").show()
    sql("SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries ORDER BY timestamp").show(truncate=False)
    print("===== Exercice 2 : namespace gold")
    sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    spark.table("polaris.silver.orders").limit(5).writeTo("polaris.gold.sample").createOrReplace()
    sql("SHOW TABLES IN polaris.gold").show()

elif STEP == "silver_analyst":
    print("===== Exercice 2 : test silver_analyst")
    print("orders silver :", spark.table("polaris.silver.orders").count())
    must_fail("Lecture de gold.sample", "SELECT count(*) FROM polaris.gold.sample")

elif STEP == "dev3":
    print("===== B9 : nettoyage")
    sql("DROP TABLE IF EXISTS polaris.gold.sample")
    sql("DROP NAMESPACE IF EXISTS polaris.gold")
    sql("SHOW TABLES IN polaris.silver").show()

print(f"===== FIN etape {STEP}")
spark.stop()