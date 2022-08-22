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
AWSGlueDataCatalog_node1660818544079 = glueContext.create_dynamic_frame.from_catalog(
    database="unsubscribedb",
    table_name="2022",
    transformation_ctx="AWSGlueDataCatalog_node1660818544079",
)

dyf_applyMapping = ApplyMapping.apply(frame = AWSGlueDataCatalog_node1660818544079, mappings = [("event_type", "String", "Event", "String" ),("facets.email_channel.mail_event.mail.common_headers.date","array","TimeStamp Desination","string"),("facets.email_channel.mail_event.mail.destination","array","Desination","string")],transformation_ctx = "dyf_applyMapping")

# Script generated for node Amazon S3
dyf_log = dyf_applyMapping.coalesce(1);
AmazonS3_node1660818546909 = glueContext.write_dynamic_frame.from_options(
    frame=dyf_log,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://deliverystream19/Output/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1660818546909",
)

job.commit()
