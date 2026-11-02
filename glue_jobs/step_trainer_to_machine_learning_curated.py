import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1793648409830 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1793648409830")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1793648373996 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1793648373996")

# Script generated for node SQL Query
SqlQuery1505 = '''
SELECT
    s.*,
    a.user AS accelerometer_user,
    a.x,
    a.y,
    a.z
FROM s
INNER JOIN a
    ON s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1793648431638 = sparkSqlQuery(glueContext, query = SqlQuery1505, mapping = {"a":accelerometer_trusted_node1793648409830, "s":step_trainer_trusted_node1793648373996}, transformation_ctx = "SQLQuery_node1793648431638")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1793648431638, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1793648279662", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1793648511881 = glueContext.getSink(path="s3://stedi-human-balance-2026-ej01/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1793648511881")
AmazonS3_node1793648511881.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1793648511881.setFormat("glueparquet", compression="snappy")
AmazonS3_node1793648511881.writeFrame(SQLQuery_node1793648431638)
job.commit()