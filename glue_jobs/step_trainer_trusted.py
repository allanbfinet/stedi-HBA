import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
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
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    step_landing = args["S3_STEP_TRAINER_LANDING"].rstrip("/") + "/"
    customers_curated = args["S3_CUSTOMERS_CURATED"].rstrip("/") + "/"
    step_trusted = args["S3_STEP_TRAINER_TRUSTED"].rstrip("/") + "/"

    # --- AWS S3 SOURCES ---
    step_landing_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"multiline": False},
        connection_options={"paths": [step_landing], "recurse": True},
        transformation_ctx="StepTrainerLanding_node",
    )

    customers_curated_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [customers_curated], "recurse": True},
        transformation_ctx="CustomersCurated_node",
    )

    step_df = step_landing_dyf.toDF()
    cust_df = customers_curated_dyf.toDF()

    # Join step trainer readings to only customers in curated set (by serialNumber)
    trusted_df = (
        step_df.join(
            cust_df.select("serialNumber").dropDuplicates(),
            on="serialNumber",
            how="inner",
        )
        .filter(F.col("sensorReadingTime").isNotNull())
        .dropDuplicates(["serialNumber", "sensorReadingTime"])
    )

    trusted_dyf = DynamicFrame.fromDF(trusted_df, glueContext, "StepTrainerTrusted_node")

    # --- AWS S3 TARGET ---
    glueContext.write_dynamic_frame.from_options(
        frame=trusted_dyf,
        connection_type="s3",
        format="parquet",
        connection_options={"path": step_trusted},
        transformation_ctx="StepTrainerTrustedSink_node",
    )

    job.commit()


if __name__ == "__main__":
    main()

