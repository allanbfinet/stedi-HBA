import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext


# Glue
GLUE_DATABASE = "stedi"
GLUE_TABLE = "accelerometer_trusted"


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

    # ---------- SOURCES ----------
    accel_landing_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"multiLine": False},   
        connection_options={"paths": [accel_landing], "recurse": True},
        transformation_ctx="AccelerometerLanding_node",
    )

    customer_trusted_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [customer_trusted], "recurse": True},
        transformation_ctx="CustomerTrusted_node",
    )

    # ---------- TRANSFORM ----------
    accel_df = accel_landing_dyf.toDF()
    cust_df = customer_trusted_dyf.toDF()

    
    cust_emails_df = cust_df.select("email").dropDuplicates(["email"])

    filtered_df = (
        accel_df.join(
            cust_emails_df,
            accel_df["user"] == cust_emails_df["email"],
            "inner"
        )
        .select(accel_df["*"])
        
    )

    accel_trusted_dyf = DynamicFrame.fromDF(
        filtered_df, glueContext, "AccelerometerTrusted_node"
    )

    # ---------- TARGET ----------
    sink = glueContext.getSink(
        connection_type="s3",
        path=accel_trusted,
        enableUpdateCatalog=True,               
        updateBehavior="UPDATE_IN_DATABASE",    
        partitionKeys=[],                       
        transformation_ctx="AccelerometerTrustedSink_node",
    )


    sink.setFormat("glueparquet")

    # Data Catalog table to create/update
    sink.setCatalogInfo(
        catalogDatabase=GLUE_DATABASE,
        catalogTableName=GLUE_TABLE
    )

    sink.writeFrame(accel_trusted_dyf)

    job.commit()


if __name__ == "__main__":
    main()
