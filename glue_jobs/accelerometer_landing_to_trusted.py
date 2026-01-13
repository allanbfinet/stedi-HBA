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
            "S3_ACCELEROMETER_LANDING",
            "S3_CUSTOMER_TRUSTED",
            "S3_ACCELEROMETER_TRUSTED",
        ],
    )

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    accel_landing = args["S3_ACCELEROMETER_LANDING"].rstrip("/") + "/"
    customer_trusted = args["S3_CUSTOMER_TRUSTED"].rstrip("/") + "/"
    accel_trusted = args["S3_ACCELEROMETER_TRUSTED"].rstrip("/") + "/"

    # --- AWS S3 SOURCES ---
    accel_landing_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"multiline": False},
        connection_options={"paths": [accel_landing], "recurse": True},
        transformation_ctx="AccelerometerLanding_node",
    )

    customer_trusted_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [customer_trusted], "recurse": True},
        transformation_ctx="CustomerTrusted_node",
    )

    # Convert to DataFrames 
    accel_df = accel_landing_dyf.toDF()
    cust_df = customer_trusted_dyf.toDF()

    
    joined = accel_df.join(
        cust_df.select("email"),
        accel_df["user"] == cust_df["email"],
        "inner",
    )

    trusted_df = (
        joined.drop(cust_df["email"])
        .filter(F.col("user").isNotNull())
        .dropDuplicates(["user", "timestamp"])
    )

    # DynamicFrame for Glue sink
    accel_trusted_dyf = DynamicFrame.fromDF(
        trusted_df, glueContext, "AccelerometerTrusted_node"
    )

    # --- AWS S3 TARGET ---
    glueContext.write_dynamic_frame.from_options(
        frame=accel_trusted_dyf,
        connection_type="s3",
        format="parquet",
        connection_options={"path": accel_trusted},
        transformation_ctx="AccelerometerTrustedSink_node",
    )

    job.commit()


if __name__ == "__main__":
    main()

