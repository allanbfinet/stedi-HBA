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
            "GLUE_DATABASE",
            "GLUE_TABLE",
        ],
    )

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    landing_path = args["S3_CUSTOMER_LANDING"].rstrip("/") + "/"
    trusted_path = args["S3_CUSTOMER_TRUSTED"].rstrip("/") + "/"

    glue_db = args.get("GLUE_DATABASE", "stedi")
    glue_table = args.get("GLUE_TABLE", "customer_trusted")

    # --- AWS S3 SOURCE ---
    customer_landing_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={"multiLine": False},
        connection_options={"paths": [landing_path], "recurse": True},
        transformation_ctx="CustomerLanding_node",
    )

    # Convert to Spark DataFrame for your existing logic
    df = customer_landing_dyf.toDF()

    df_trusted = df.filter(F.col("shareWithResearchAsOfDate").isNotNull())

    
    customer_trusted_dyf = DynamicFrame.fromDF(
        df_trusted, glueContext, "CustomerTrusted_node"
    )

    # --- AWS S3 TARGET ---
    sink = glueContext.getSink(
        path=trusted_path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx="CustomerTrustedSink_node",
    )
    sink.setCatalogInfo(catalogDatabase=glue_db, catalogTableName=glue_table)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(customer_trusted_dyf)

    job.commit()


if __name__ == "__main__":
    main()
