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
            "S3_CUSTOMER_LANDING",
            "S3_CUSTOMER_TRUSTED",
        ],
    )

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    landing_path = args["S3_CUSTOMER_LANDING"].rstrip("/") + "/"
    trusted_path = args["S3_CUSTOMER_TRUSTED"].rstrip("/") + "/"

    # --- AWS S3 SOURCE ---
    customer_landing_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"multiline": False},
        connection_options={"paths": [landing_path], "recurse": True},
        transformation_ctx="CustomerLanding_node",
    )

    # Convert to Spark DataFrame
    df = customer_landing_dyf.toDF()


    df_trusted = (
        df.filter(F.col("shareWithResearchAsOfDate").isNotNull())
        .dropDuplicates(["email"])
    )

    # DynamicFrame for Glue sink
    customer_trusted_dyf = DynamicFrame.fromDF(
        df_trusted, glueContext, "CustomerTrusted_node"
    )

    # AWS S3 TARGET
    glueContext.write_dynamic_frame.from_options(
        frame=customer_trusted_dyf,
        connection_type="s3",
        format="parquet",
        connection_options={"path": trusted_path},
        transformation_ctx="CustomerTrustedSink_node",
    )

    job.commit()


if __name__ == "__main__":
    main()

