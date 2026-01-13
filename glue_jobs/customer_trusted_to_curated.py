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
            "S3_CUSTOMER_TRUSTED",
            "S3_ACCELEROMETER_TRUSTED",
            "S3_CUSTOMERS_CURATED",
        ],
    )

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    customer_trusted = args["S3_CUSTOMER_TRUSTED"].rstrip("/") + "/"
    accelerometer_trusted = args["S3_ACCELEROMETER_TRUSTED"].rstrip("/") + "/"
    customers_curated = args["S3_CUSTOMERS_CURATED"].rstrip("/") + "/"

    # --- AWS S3 SOURCES ---
    customer_trusted_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [customer_trusted], "recurse": True},
        transformation_ctx="CustomerTrusted_node",
    )

    accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [accelerometer_trusted], "recurse": True},
        transformation_ctx="AccelerometerTrusted_node",
    )

    cust_df = customer_trusted_dyf.toDF()
    accel_df = accelerometer_trusted_dyf.toDF()

    # Keep only customers who actually have accelerometer data
    curated_df = (
        cust_df.join(
            accel_df.select("user").dropDuplicates(),
            cust_df["email"] == accel_df["user"],
            "inner",
        )
        .drop(accel_df["user"])
        .dropDuplicates(["email"])
    )

    curated_dyf = DynamicFrame.fromDF(curated_df, glueContext, "CustomersCurated_node")

    # --- AWS S3 TARGET ---
    glueContext.write_dynamic_frame.from_options(
        frame=curated_dyf,
        connection_type="s3",
        format="parquet",
        connection_options={"path": customers_curated},
        transformation_ctx="CustomersCuratedSink_node",
    )

    job.commit()


if __name__ == "__main__":
    main()

