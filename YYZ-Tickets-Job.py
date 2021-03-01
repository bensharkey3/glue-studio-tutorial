import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

def Aggregate_Tickets(glueContext, dfc) -> DynamicFrameCollection:
    selected = dfc.select(list(dfc.keys())[0]).toDF()
    selected.createOrReplaceTempView("ticketcount")
    totals = spark.sql("select court_location as location, infraction_description as infraction, count(infraction_code) as total  FROM ticketcount group by infraction_description, infraction_code, court_location order by court_location asc")
    results = DynamicFrame.fromDF(totals, glueContext, "results")
    return DynamicFrameCollection({"results": results}, glueContext)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "yyz-tickets", table_name = "trials", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "yyz-tickets", table_name = "trials", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("court_date", "date", "court_date", "date"), ("court_location", "string", "court_location", "string"), ("court_room", "string", "court_room", "string"), ("court_time", "int", "court_time", "int"), ("parking_ticket_number", "long", "parking_ticket_number", "int"), ("infraction_date", "date", "infraction_date", "date"), ("first_3_letters_name", "string", "first_3_letters_name", "string"), ("sentence", "string", "sentence", "string")], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = DataSource0]
Transform1 = ApplyMapping.apply(frame = DataSource0, mappings = [("court_date", "date", "court_date", "date"), ("court_location", "string", "court_location", "string"), ("court_room", "string", "court_room", "string"), ("court_time", "int", "court_time", "int"), ("parking_ticket_number", "long", "parking_ticket_number", "int"), ("infraction_date", "date", "infraction_date", "date"), ("first_3_letters_name", "string", "first_3_letters_name", "string"), ("sentence", "string", "sentence", "string")], transformation_ctx = "Transform1")
## @type: DataSource
## @args: [database = "yyz-tickets", table_name = "tickets", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "yyz-tickets", table_name = "tickets", transformation_ctx = "DataSource1")
## @type: ApplyMapping
## @args: [mappings = [("tag_number_masked", "string", "tag_number_masked", "string"), ("date_of_infraction", "string", "date_of_infraction", "string"), ("ticket_date", "string", "ticket_date", "string"), ("ticket_number", "decimal", "ticket_number", "int"), ("officer", "decimal", "officer", "decimal"), ("infraction_code", "decimal", "infraction_code", "decimal"), ("infraction_description", "string", "infraction_description", "string"), ("set_fine_amount", "decimal", "set_fine_amount", "decimal"), ("time_of_infraction", "decimal", "time_of_infraction", "decimal")], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [frame = DataSource1]
Transform4 = ApplyMapping.apply(frame = DataSource1, mappings = [("tag_number_masked", "string", "tag_number_masked", "string"), ("date_of_infraction", "string", "date_of_infraction", "string"), ("ticket_date", "string", "ticket_date", "string"), ("ticket_number", "decimal", "ticket_number", "int"), ("officer", "decimal", "officer", "decimal"), ("infraction_code", "decimal", "infraction_code", "decimal"), ("infraction_description", "string", "infraction_description", "string"), ("set_fine_amount", "decimal", "set_fine_amount", "decimal"), ("time_of_infraction", "decimal", "time_of_infraction", "decimal")], transformation_ctx = "Transform4")
## @type: Join
## @args: [keys2 = ["parking_ticket_number"], keys1 = ["ticket_number"], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame1 = Transform4, frame2 = Transform1]
Transform2 = Join.apply(frame1 = Transform4, frame2 = Transform1, keys2 = ["parking_ticket_number"], keys1 = ["ticket_number"], transformation_ctx = "Transform2")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform2": Transform2}, glueContext), className = Aggregate_Tickets, transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [dfc = Transform2]
Transform3 = Aggregate_Tickets(glueContext, DynamicFrameCollection({"Transform2": Transform2}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform3.keys())[0], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [dfc = Transform3]
Transform0 = SelectFromCollection.apply(dfc = Transform3, key = list(Transform3.keys())[0], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "parquet", connection_options = {"path": "s3://glue-studio-blog-537808241319/parking_tickets_count/", "compression": "gzip", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "parquet", connection_options = {"path": "s3://glue-studio-blog-537808241319/parking_tickets_count/", "compression": "gzip", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()