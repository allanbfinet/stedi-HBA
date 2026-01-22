import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext


# Glue
GLUE_DATABASE = "stedi"
GLUE_TABLE = "step_trainer_trusted"


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
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    step_landing = args["S3_STEP_TRAINER_LANDING"].rstrip("/") + "/"
    customers_curated = args["S3_CUSTOMERS_CURATED"].rstrip("/") + "/"
    step_trusted = args["S3_STEP_TRAINER_TRUSTED"].rstrip("/") + "/"

    # --- SOURCES ---
    step_landing_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"multiLine": False},  # ✅ boolean
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

    curated_serials = cust_df.select("serialNumber").dropDuplicates(["serialNumber"])

    trusted_df = (
        step_df.join(curated_serials, on="serialNumber", how="inner")
        .select(step_df["*"])
    )

    trusted_dyf = DynamicFrame.fromDF(trusted_df, glueContext, "StepTrainerTrusted_node")

    # --- TARGET ---
    sink = glueContext.getSink(
        connection_type="s3",
        path=step_trusted,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        transformation_ctx="StepTrainerTrustedSink_node",
    )
    sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName=GLUE_TABLE)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(trusted_dyf)

    job.commit()


if __name__ == "__main__":
    main()
