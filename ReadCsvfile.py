from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
import sys
from awsglue.utils import getResolvedOptions

glueContext = GlueContext(SparkContext.getOrCreate())
glueJob = Job(glueContext)
args = getResolvedOptions(sys.argv,['JOB_NAME'])

glueJob.init(args['JOB_NAME'],args)
sparkSession = glueContext.sparkSession

df = sparkSession.read.csv("s3a://test-target")
df.show()
glueJob.commit()
