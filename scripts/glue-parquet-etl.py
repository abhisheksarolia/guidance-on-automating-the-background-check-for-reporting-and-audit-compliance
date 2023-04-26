import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import year, month, dayofmonth, hour, explode, col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'export_bucket', 'processed_bucket'])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
logger.info("info message")
logger.warn("warn message")
logger.error("error message")

export_bucket_name = args['export_bucket']
logger.info(export_bucket_name)
processed_bucket_name = args['processed_bucket']
logger.info(processed_bucket_name)
full_process_path = processed_bucket_name + "/glue-job-output/full_export"
revision_process_path = processed_bucket_name + "/glue-job-output/just_revisions"
logger.info(revision_process_path)

def AddTime(r):
    doc_list = []
    r["year"] = r["blockTimestamp"].year
    r["month"] = r["blockTimestamp"].month
    r["day"] = r["blockTimestamp"].day
    r["hour"] = r["blockTimestamp"].hour
    for each in r["transactionInfo"]["documents"]:
        doc_list.append({"docId":each,
                         "tableName":r["transactionInfo"]["documents"][each]["tableName"],
                         "tableId":r["transactionInfo"]["documents"][each]["tableId"],
                         "statements":r["transactionInfo"]["documents"][each]["statements"]})
    r["transactionInfo"]["documents"] = doc_list
    return r

def just_rev(r):
    if 'revisions' in r:
        revisions = r['revisions']
        r['revisions'] = []
        for revision in revisions:
            if 'metadata' in revision:
                revision['qldb_table'] = r['transactionInfo']['documents'][revision['metadata']['id']]['tableName']
                r['revisions'].append(revision)
    return r

DataSource0 = glueContext.create_dynamic_frame.from_options(connection_type = "s3", format = "ion", connection_options = {"paths": [export_bucket_name], "recurse":True, "exclusions":["**.json","**.manifest"], "groupFiles": "inPartition", "groupSize": "134217728", "ignoreCorruptFiles":True}, transformation_ctx = "DataSource0")
DataSource1 = Map.apply(frame = DataSource0, f = AddTime)
DataSource2 = Map.apply(frame = DataSource0, f = just_rev)
Transform0 = ApplyMapping.apply(frame = DataSource1, mappings = [("blockAddress.strandId", "string", "blockAddress.strandId", "string"), ("blockAddress.sequenceNo", "int", "blockAddress.sequenceNo", "int"), ("transactionId", "string", "transactionId", "string"), ("blockTimestamp", "timestamp", "blockTimestamp", "timestamp"), ("blockHash", "binary", "blockHash", "binary"), ("entriesHash", "binary", "entriesHash", "binary"), ("previousBlockHash", "binary", "previousBlockHash", "binary"), ("entriesHashList", "array", "entriesHashList", "array"), ("transactionInfo.statements", "array", "transactionInfo.statements", "array"), ("transactionInfo.documents", "array", "transactionInfo.documents", "array"), ("revisions", "array", "revisions", "array"), ("year", "int", "year", "int"), ("month", "int", "month", "int"), ("day", "int", "day", "int"), ("hour", "int", "hour", "int")], transformation_ctx = "Transform0")
Transform1 = ApplyMapping.apply(frame = DataSource2, mappings = [("revisions", "array", "revisions", "array")], transformation_ctx = "Transform1")
Transform2 = Transform1.toDF().select(explode(col("revisions")).alias("collection")).select("collection.*")
Transform3 = DynamicFrame.fromDF(Transform2, glueContext, "Transform3")
# DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "glueparquet", connection_options = {"path": full_process_path, "partitionKeys": ["year", "month", "day", "hour"], "compression": "snappy"}, transformation_ctx = "DataSink0")
DataSink1 = glueContext.write_dynamic_frame.from_options(frame = Transform3, connection_type = "s3", format = "glueparquet", connection_options = {"path": revision_process_path, "partitionKeys": ["qldb_table"], "compression": "snappy"}, transformation_ctx = "DataSink1")

job.commit()