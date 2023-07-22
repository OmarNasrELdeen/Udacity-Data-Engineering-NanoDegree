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

# Script generated for node step trainer landing
steptrainerlanding_node1690058866614 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lakehouse/step-trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainerlanding_node1690058866614",
)

# Script generated for node Customer currated
Customercurrated_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="Customercurrated_node1",
)

# Script generated for node Join
Join_node1690058975631 = Join.apply(
    frame1=Customercurrated_node1,
    frame2=steptrainerlanding_node1690058866614,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1690058975631",
)

# Script generated for node Drop customer fields
Dropcustomerfields_node1690058962703 = DropFields.apply(
    frame=Join_node1690058975631,
    paths=[
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
        "sharewithresearchasofdate",
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
    ],
    transformation_ctx="Dropcustomerfields_node1690058962703",
)

# Script generated for node ste trainer trusted
stetrainertrusted_node1690058952965 = glueContext.write_dynamic_frame.from_catalog(
    frame=Dropcustomerfields_node1690058962703,
    database="stedi",
    table_name="steptrainer_trusted",
    transformation_ctx="stetrainertrusted_node1690058952965",
)

job.commit()
