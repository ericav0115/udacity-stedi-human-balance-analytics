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

# Script generated for node customer_curated
customer_curated_node1793647959374 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="customer_curated_node1793647959374")

# Script generated for node step_trainer
step_trainer_node1793647924328 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_node1793647924328")

# Script generated for node SQL Query
SqlQuery1161 = '''
SELECT s.*
FROM s
LEFT SEMI JOIN c
    ON s.serialnumber = c.serialnumber

'''
SQLQuery_node1793648009960 = sparkSqlQuery(glueContext, query = SqlQuery1161, mapping = {"c":customer_curated_node1793647959374, "s":step_trainer_node1793647924328}, transformation_ctx = "SQLQuery_node1793648009960")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1793648009960, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1793647450303", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1793648154472 = glueContext.getSink(path="s3://stedi-human-balance-2026-ej01/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1793648154472")
AmazonS3_node1793648154472.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1793648154472.setFormat("glueparquet", compression="snappy")
AmazonS3_node1793648154472.writeFrame(SQLQuery_node1793648009960)
job.commit()