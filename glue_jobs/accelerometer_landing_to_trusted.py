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

# Script generated for node accelerometer_landing
accelerometer_landing_node1793645954512 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1793645954512")

# Script generated for node customer_trusted
customer_trusted_node1793645989780 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1793645989780")

# Script generated for node SQL Query
SqlQuery1260 = '''

SELECT
    a.*
FROM a
INNER JOIN (
    SELECT DISTINCT email
    FROM c
    WHERE email IS NOT NULL
) consented
    ON a.user = consented.email

'''
SQLQuery_node1793646012154 = sparkSqlQuery(glueContext, query = SqlQuery1260, mapping = {"c":customer_trusted_node1793645989780, "a":accelerometer_landing_node1793645954512}, transformation_ctx = "SQLQuery_node1793646012154")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1793646012154, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1793645888139", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1793646064086 = glueContext.getSink(path="s3://stedi-human-balance-2026-ej01/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1793646064086")
AmazonS3_node1793646064086.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1793646064086.setFormat("glueparquet", compression="snappy")
AmazonS3_node1793646064086.writeFrame(SQLQuery_node1793646012154)
job.commit()