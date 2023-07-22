import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer landing
accelerometerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlanding_node1",
)

# Script generated for node customer trusted
customertrusted_node1690053873164 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1690053873164",
)

# Script generated for node filter on accerlometer data
filteronaccerlometerdata_node1690053868670 = Join.apply(
    frame1=accelerometerlanding_node1,
    frame2=customertrusted_node1690053873164,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="filteronaccerlometerdata_node1690053868670",
)

# Script generated for node Drop accelerometer fields
Dropaccelerometerfields_node1690054831958 = DropFields.apply(
    frame=filteronaccerlometerdata_node1690053868670,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="Dropaccelerometerfields_node1690054831958",
)

# Script generated for node customer currated
customercurrated_node1690057364184 = glueContext.write_dynamic_frame.from_catalog(
    frame=Dropaccelerometerfields_node1690054831958,
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customercurrated_node1690057364184",
)

job.commit()
