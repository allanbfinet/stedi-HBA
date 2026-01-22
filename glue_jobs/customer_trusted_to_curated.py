import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext


# Glue database/table names
GLUE_DATABASE = "stedi"
GLUE_TABLE = "customers_curated"


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_CUSTOMER_TRUSTED",
            "S3_ACCELEROMETER_TRUSTED",
            "S3_STEP_TRAINER_LANDING",
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
    step_trainer_landing = args["S3_STEP_TRAINER_LANDING"].rstrip("/") + "/"
    customers_curated = args["S3_CUSTOMERS_CURATED"].rstrip("/") + "/"

    # --- SOURCES ---
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

    step_trainer_landing_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"multiLine": False},  
        connection_options={"paths": [step_trainer_landing], "recurse": True},
        transformation_ctx="StepTrainerLanding_node",
    )

    cust_df = customer_trusted_dyf.toDF()
    accel_df = accelerometer_trusted_dyf.toDF()
    step_df = step_trainer_landing_dyf.toDF()

    # Distinct accelerometer users
    accel_users_df = accel_df.select("user").dropDuplicates(["user"])

    # Distinct step trainer serial numbers
    step_serials_df = step_df.select("serialNumber").dropDuplicates(["serialNumber"])

    curated_df = (
        cust_df.join(accel_users_df, cust_df["email"] == accel_users_df["user"], "inner")
        .drop(accel_users_df["user"])
        .join(step_serials_df, on="serialNumber", how="inner")
        .dropDuplicates(["email"]) 
    )

    curated_dyf = DynamicFrame.fromDF(curated_df, glueContext, "CustomersCurated_node")

    # --- TARGET ---
    sink = glueContext.getSink(
        connection_type="s3",
        path=customers_curated,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        transformation_ctx="CustomersCuratedSink_node",
    )
    sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName=GLUE_TABLE)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(curated_dyf)

    job.commit()


if __name__ == "__main__":
    main()
