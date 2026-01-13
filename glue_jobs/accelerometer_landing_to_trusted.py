import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_ACCELEROMETER_LANDING",
            "S3_CUSTOMER_TRUSTED",
            "S3_ACCELEROMETER_TRUSTED",
        ],
    )

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    accel_landing = args["S3_ACCELEROMETER_LANDING"].rstrip("/") + "/"
    customer_trusted = args["S3_CUSTOMER_TRUSTED"].rstrip("/") + "/"
    accel_trusted = args["S3_ACCELEROMETER_TRUSTED"].rstrip("/") + "/"

    accel_df = spark.read.option("multiline", "false").json(accel_landing)
    cust_df = spark.read.parquet(customer_trusted)

    # In STEDI: accelerometer uses column "user" (email). Customers use "email".
    joined = accel_df.join(cust_df.select("email"), accel_df["user"] == cust_df["email"], "inner")

    # Clean + dedupe
    trusted_df = (
        joined.drop(cust_df["email"])
        .filter(F.col("user").isNotNull())
        .dropDuplicates(["user", "timestamp"])
    )

    trusted_df.coalesce(1).write.mode("overwrite").parquet(accel_trusted)

    job.commit()


if __name__ == "__main__":
    main()
