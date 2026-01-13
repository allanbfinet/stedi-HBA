import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    landing_path = args["S3_CUSTOMER_LANDING"].rstrip("/") + "/"
    trusted_path = args["S3_CUSTOMER_TRUSTED"].rstrip("/") + "/"

    # Read landing customers (JSON)
    df = spark.read.option("multiline", "false").json(landing_path)

    
    df_trusted = (
        df.filter(F.col("shareWithResearchAsOfDate").isNotNull())
        .dropDuplicates(["email"])
    )

    (
        df_trusted.coalesce(1)
        .write.mode("overwrite")
        .parquet(trusted_path)
    )

    job.commit()


if __name__ == "__main__":
    main()
