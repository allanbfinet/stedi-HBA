import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext


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
        format_options={"multiLine": "false"},
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

    curated_serials = cust_df.select("serialNumber").dropDuplicates()

    trusted_df = (
        step_df.join(curated_serials, on="serialNumber", how="inner")
        .select(step_df["*"])  # keep only step trainer columns
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
