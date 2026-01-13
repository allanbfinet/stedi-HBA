import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_CUSTOMER_TRUSTED",
            "S3_ACCELEROMETER_TRUSTED",
            "S3_CUSTOMERS_CURATED",
        ],
    )

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    customer_trusted = args["S3_CUSTOMER_TRUSTED"].rstrip("/") + "/"
    accelerometer_trusted = args["S3_ACCELEROMETER_TRUSTED"].rstrip("/") + "/"
    customers_curated = args["S3_CUSTOMERS_CURATED"].rstrip("/") + "/"

    cust_df = spark.read.parquet(customer_trusted)
    accel_df = spark.read.parquet(accelerometer_trusted)

    # Keep only customers who actually have accelerometer data
    curated_df = (
        cust_df.join(accel_df.select("user").dropDuplicates(), cust_df["email"] == accel_df["user"], "inner")
        .drop(accel_df["user"])
        .dropDuplicates(["email"])
    )

    curated_df.coalesce(1).write.mode("overwrite").parquet(customers_curated)

    job.commit()


if __name__ == "__main__":
    main()
