import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer landing zone
customerlandingzone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lakehouse/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customerlandingzone_node1",
)

# Script generated for node FilterOnCustomers
FilterOnCustomers_node1690052548955 = Filter.apply(
    frame=customerlandingzone_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="FilterOnCustomers_node1690052548955",
)

# Script generated for node customer trusted zone
customertrustedzone_node1690057050470 = glueContext.write_dynamic_frame.from_catalog(
    frame=FilterOnCustomers_node1690052548955,
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customertrustedzone_node1690057050470",
)

job.commit()
