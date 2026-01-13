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
            "S3_ACCELEROMETER_TRUSTED",
            "S3_STEP_TRAINER_TRUSTED",
            "S3_CUSTOMERS_CURATED",
            "S3_MACHINE_LEARNING_CURATED",
        ],
    )

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    accel_trusted = args["S3_ACCELEROMETER_TRUSTED"].rstrip("/") + "/"
    step_trusted = args["S3_STEP_TRAINER_TRUSTED"].rstrip("/") + "/"
    customers_curated = args["S3_CUSTOMERS_CURATED"].rstrip("/") + "/"
    ml_curated = args["S3_MACHINE_LEARNING_CURATED"].rstrip("/") + "/"

    accel_df = spark.read.parquet(accel_trusted)
    step_df = spark.read.parquet(step_trusted)
    cust_df = spark.read.parquet(customers_curated)

    step_with_user = (
        step_df.join(
            cust_df.select("email", "serialNumber"),
            on="serialNumber",
            how="inner",
        )
        .withColumnRenamed("email", "user")
    )


    joined = step_with_user.join(
        accel_df,
        (step_with_user["user"] == accel_df["user"])
        & (step_with_user["sensorReadingTime"] == accel_df["timestamp"]),
        "inner",
    )

    ml_df = (
        joined.select(
            step_with_user["user"].alias("user"),
            step_with_user["serialNumber"].alias("serialNumber"),
            step_with_user["sensorReadingTime"].alias("sensorReadingTime"),
            accel_df["x"].alias("x"),
            accel_df["y"].alias("y"),
            accel_df["z"].alias("z"),
            step_with_user["distanceFromObject"].alias("distanceFromObject"),
        )
        .filter(F.col("user").isNotNull())
        .dropDuplicates(["user", "sensorReadingTime"])
    )

    ml_df.coalesce(1).write.mode("overwrite").parquet(ml_curated)

    job.commit()


if __name__ == "__main__":
    main()
