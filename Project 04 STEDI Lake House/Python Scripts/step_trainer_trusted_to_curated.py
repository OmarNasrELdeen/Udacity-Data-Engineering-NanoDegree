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

# Script generated for node step trainer trusted
steptrainertrusted_node1690060010428 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="steptrainer_trusted",
    transformation_ctx="steptrainertrusted_node1690060010428",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1",
)

# Script generated for node Join
Join_node1690060004737 = Join.apply(
    frame1=Accelerometertrusted_node1,
    frame2=steptrainertrusted_node1690060010428,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1690060004737",
)

# Script generated for node Drop Fields
DropFields_node1690060200347 = DropFields.apply(
    frame=Join_node1690060004737,
    paths=["user"],
    transformation_ctx="DropFields_node1690060200347",
)

# Script generated for node machine learning curated
machinelearningcurated_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1690060200347,
    database="stedi",
    table_name="machine_learning_curated",
    transformation_ctx="machinelearningcurated_node3",
)

job.commit()
