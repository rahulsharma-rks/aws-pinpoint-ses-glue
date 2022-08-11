import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
dfc_root_table_name = "root"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1659938071518 = glueContext.create_dynamic_frame.from_catalog(
    database="pinpointglue",
    table_name="2022",
    transformation_ctx="AWSGlueDataCatalog_node1659938071518",
)
dfc = Relationalize.apply(frame = AWSGlueDataCatalog_node1659938071518, name = dfc_root_table_name, transformation_ctx = "dfc")
blogdata = dfc.select(dfc_root_table_name)

# Script generated for node Amazon S3
AmazonS3_node1659938078097 = glueContext.write_dynamic_frame.from_options(
    frame=blogdata,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://deliverystream19/Output/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1659938078097",
)

job.commit()
