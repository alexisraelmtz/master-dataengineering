import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1654545178421 = glueContext.create_dynamic_frame.from_catalog(
    database="alex_mtz_db",
    table_name="train_departures_raw",
    transformation_ctx="AWSGlueDataCatalog_node1654545178421",
)

# Script generated for node SQL
SqlQuery0 = """
select
    origin.location[0].locationName as origin,
    destination.location[0].locationName as destination,
    std as current_station_time,
    etd as status,
    ingest_year,
    ingest_month,
    ingest_day,
    ingest_hour
from myDataSource
"""
SQL_node1654545190164 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": AWSGlueDataCatalog_node1654545178421},
    transformation_ctx="SQL_node1654545190164",
)

# Script generated for node Amazon S3
AmazonS3_node1654545490168 = glueContext.getSink(
    path="s3://enroute-bucket/assignment2/clean_alex_martinez/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["ingest_year", "ingest_month", "ingest_day", "ingest_hour"],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1654545490168",
)
AmazonS3_node1654545490168.setCatalogInfo(
    catalogDatabase="alex_mtz_db", catalogTableName="train_departures_clean"
)
AmazonS3_node1654545490168.setFormat("json")
AmazonS3_node1654545490168.writeFrame(SQL_node1654545190164)
job.commit()

print("Exit code - 0\nSuccessful Deploy - AIMC")
print("glueContext:", glueContext)
