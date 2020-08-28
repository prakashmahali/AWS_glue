from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
import sys
from awsglue.utils import getResolvedOptions

glueContext = GlueContext(SparkContext.getOrCreate())
glueJob = Job(glueContext)
args = getResolvedOptions(sys.argv,['JOB_NAME'])

glueJob.init(args['JOB_NAME'],args)
#sparkSession = glueContext.sparkSession
spark = glueContext.spark_session

#df = sparkSession.read.csv("s3a://prakashmahali")
#df.show()

#dfnew = spark.read.option("header","true").option("delimiter", ",").csv("s3a://pkm")
df = spark.read.option("header","true").format("csv").load("s3a://pkm")
df.show(2)
glueJob.commit()
