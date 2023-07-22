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

# Script generated for node filter on trusted customer data
filterontrustedcustomerdata_node1690053868670 = Join.apply(
    frame1=accelerometerlanding_node1,
    frame2=customertrusted_node1690053873164,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="filterontrustedcustomerdata_node1690053868670",
)

# Script generated for node Drop customer fields
Dropcustomerfields_node1690054831958 = DropFields.apply(
    frame=filterontrustedcustomerdata_node1690053868670,
    paths=[
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "customername",
    ],
    transformation_ctx="Dropcustomerfields_node1690054831958",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1690057364184 = glueContext.write_dynamic_frame.from_catalog(
    frame=Dropcustomerfields_node1690054831958,
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1690057364184",
)

job.commit()
