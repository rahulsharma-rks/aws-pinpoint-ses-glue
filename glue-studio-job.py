import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
dfc_root_table_name = "root"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1660200579339 = glueContext.create_dynamic_frame.from_catalog(
    database="pinpointglue",
    table_name="2022",
    transformation_ctx="AWSGlueDataCatalog_node1660200579339",
)

# dfc = Relationalize.apply(frame = AWSGlueDataCatalog_node1660200579339, name = dfc_root_table_name, transformation_ctx = "dfc")
# tabledata = dfc.select(dfc_root_table_name)
#df = AWSGlueDataCatalog_node1660200579339.toDF()

#dyf_selectFields = SelectFields.apply(frame = blogdata, paths=['event_type','attributes.user_id'])
# selected_attr = ['event_type','event_timestamp','facets.email_channel.mail_event.mail.destination']
# dyf_selectFields = SelectFields.apply(frame = AWSGlueDataCatalog_node1660200579339, paths = selected_attr)

dyf_applyMapping = ApplyMapping.apply(frame = AWSGlueDataCatalog_node1660200579339, mappings = [("event_type", "String", "Event", "String" ),("facets.email_channel.mail_event.mail.common_headers.date","array","TimeStamp Desination","string"),("facets.email_channel.mail_event.mail.destination","array","Desination","string")],transformation_ctx = "dyf_applyMapping")
# tabledata = dyf_applyMapping(dfc_root_table_name)

# Script generated for node Amazon S3
AmazonS3_node1660200580785 = glueContext.write_dynamic_frame.from_options(
    frame=dyf_applyMapping,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://deliverystream19/Output/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1660200580785",
)

job.commit()
