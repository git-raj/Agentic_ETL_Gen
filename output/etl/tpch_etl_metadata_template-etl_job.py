
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("ETL Job").getOrCreate()

    # AWS Glue-specific setup
    import sys
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions

    glueContext = GlueContext(spark.sparkContext)


    # Read source data
    source_df = spark.table("customer")
    target_df = source_df

    # Apply transformations
    target_df = target_df.withColumn("customer_id", source_df["c_custkey"])
    target_df = target_df.withColumn("customer_name", source_df["c_name"])
    target_df = target_df.withColumn("order_id", source_df["o_orderkey"])

    # Write target data
    glueContext.write_dynamic_frame.from_options(frame=DynamicFrame.fromDF(target_df, glueContext, "target"), connection_type="s3", connection_options={"path": "s3://your-bucket/customer"}, format="parquet")

    spark.stop()

if __name__ == "__main__":
    main()
