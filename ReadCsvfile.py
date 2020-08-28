from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
import sys
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

glueContext = GlueContext(SparkContext.getOrCreate())
glueJob = Job(glueContext)
args = getResolvedOptions(sys.argv,['JOB_NAME'])

glueJob.init(args['JOB_NAME'],args)
#sparkSession = glueContext.sparkSession
spark = glueContext.spark_session

#df = sparkSession.read.csv("s3a://pkm")
#df.show()

#dfnew = spark.read.option("header","true").option("delimiter", ",").csv("s3a://pkm")
df = spark.read.option("header","true").format("csv").load("s3a://pkm
#inputGDF = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": ["s3://pkm"], "recurse":True}, format = "csv")
#df=inputGDF.toDF()
df.show(2)

dynamic_dframe = DynamicFrame.fromDF(df, glueContext, "dynamic_df")
 
##Write the DynamicFrame as a file in CSV format to a folder in an S3 bucket.
##It is possible to write to any Amazon data store (SQL Server, Redshift, etc) by using any previously defined connections.
retDatasink4 = glueContext.write_dynamic_frame.from_options(frame = dynamic_dframe, connection_type = "s3", connection_options = {"path": "s3://pkm-target"}, format = "csv", transformation_ctx = "datasink4")

glueJob.commit()
