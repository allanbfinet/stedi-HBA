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
            "S3_STEP_TRAINER_LANDING",
            "S3_CUSTOMERS_CURATED",
            "S3_STEP_TRAINER_TRUSTED",
        ],
    )

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    step_landing = args["S3_STEP_TRAINER_LANDING"].rstrip("/") + "/"
    customers_curated = args["S3_CUSTOMERS_CURATED"].rstrip("/") + "/"
    step_trusted = args["S3_STEP_TRAINER_TRUSTED"].rstrip("/") + "/"

    step_df = spark.read.option("multiline", "false").json(step_landing)
    cust_df = spark.read.parquet(customers_curated)

    # Step Trainer typically has "serialNumber"; customers have "serialNumber"
    trusted_df = (
        step_df.join(cust_df.select("serialNumber").dropDuplicates(), on="serialNumber", how="inner")
        .filter(F.col("sensorReadingTime").isNotNull())
        .dropDuplicates(["serialNumber", "sensorReadingTime"])
    )

    trusted_df.coalesce(1).write.mode("overwrite").parquet(step_trusted)

    job.commit()


if __name__ == "__main__":
    main()
