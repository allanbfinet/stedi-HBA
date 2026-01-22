import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# Glue
GLUE_DATABASE = "stedi"
GLUE_TABLE = "machine_learning_curated"


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_ACCELEROMETER_TRUSTED",
            "S3_STEP_TRAINER_TRUSTED",
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
    ml_curated = args["S3_MACHINE_LEARNING_CURATED"].rstrip("/") + "/"

    # --- SOURCES ---
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

    acc = accel_dyf.toDF()
    st = step_dyf.toDF()

    # Ensure join keys are same type
    acc = acc.withColumn("timestamp_long", F.col("timestamp").cast("long"))
    st = st.withColumn("sensorReadingTime_long", F.col("sensorReadingTime").cast("long"))

    # Join 
    joined = acc.join(
        st,
        acc["timestamp_long"] == st["sensorReadingTime_long"],
        "inner",
    )

    # Select ML training columns
    ml_df = joined.select(
        acc["user"].alias("user"),
        acc["timestamp"].cast("long").alias("timestamp"),
        acc["x"].alias("x"),
        acc["y"].alias("y"),
        acc["z"].alias("z"),
        st["sensorReadingTime"].cast("long").alias("sensorReadingTime"),
        st["distanceFromObject"].alias("distanceFromObject"),
    ).dropDuplicates(
        ["user", "timestamp", "x", "y", "z", "sensorReadingTime", "distanceFromObject"]
    )

    ml_dyf = DynamicFrame.fromDF(ml_df, glueContext, "MachineLearningCurated_node")

    # --- TARGET ---
    sink = glueContext.getSink(
        connection_type="s3",
        path=ml_curated,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        transformation_ctx="MachineLearningCuratedSink_node",
    )
    sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName=GLUE_TABLE)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(ml_dyf)

    job.commit()


if __name__ == "__main__":
    main()
