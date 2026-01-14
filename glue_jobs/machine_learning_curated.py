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
            "S3_ACCELEROMETER_TRUSTED",
            "S3_STEP_TRAINER_TRUSTED",
            "S3_CUSTOMERS_CURATED",
            "S3_MACHINE_LEARNING_CURATED",
        ],
    )

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    accel_trusted = args["S3_ACCELEROMETER_TRUSTED"].rstrip("/") + "/"
    step_trusted = args["S3_STEP_TRAINER_TRUSTED"].rstrip("/") + "/"
    customers_curated = args["S3_CUSTOMERS_CURATED"].rstrip("/") + "/"
    ml_curated = args["S3_MACHINE_LEARNING_CURATED"].rstrip("/") + "/"

    # --- AWS S3 SOURCES ---
    accel_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [accel_trusted], "recurse": True},
        transformation_ctx="AccelerometerTrusted_node",
    )

    step_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [step_trusted], "recurse": True},
        transformation_ctx="StepTrainerTrusted_node",
    )

    cust_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [customers_curated], "recurse": True},
        transformation_ctx="CustomersCurated_node",
    )

    accel_df = accel_dyf.toDF()
    step_df = step_dyf.toDF()
    cust_df = cust_dyf.toDF()

    # Step trainer -> attach user (email) by serialNumber using customers_curated
    step_with_user = (
        step_df.join(
            cust_df.select("email", "serialNumber"),
            on="serialNumber",
            how="inner",
        )
        .withColumnRenamed("email", "user")
    )

<<<<<<< HEAD
    # Join step trainer readings to accelerometer at same timestamp for same user
=======
    # Join step trainer readings to accelerometer
>>>>>>> d57cc7c (Update Glue scripts to use S3 DynmicFrame sources)
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

    ml_dyf = DynamicFrame.fromDF(ml_df, glueContext, "MachineLearningCurated_node")

    # --- AWS S3 TARGET ---
    glueContext.write_dynamic_frame.from_options(
        frame=ml_dyf,
        connection_type="s3",
        format="parquet",
        connection_options={"path": ml_curated},
        transformation_ctx="MachineLearningCuratedSink_node",
    )

    job.commit()


if __name__ == "__main__":
    main()

