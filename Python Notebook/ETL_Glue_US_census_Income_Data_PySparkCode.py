import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "yelp", table_name = "countywise_average_income_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "yelp", table_name = "countywise_average_income_csv", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("col0", "string", "County", "string"), ("col1", "string", "State", "string"), ("col2", "long", "Estimate_Households_Total", "long"), ("col3", "double", "Estimate_Households_Total_Less_than_10000", "double"), ("col4", "double", "Estimate_Households_Total_10000_14999", "double"), ("col5", "double", "Estimate_Households_Total_15000_24999", "double"), ("col6", "double", "Estimate_Households_Total_25000_34999", "double"), ("col7", "double", "Estimate_Households_Total_35000_49999", "double"), ("col8", "double", "Estimate_Households_Total_50000_74999", "double"), ("col9", "double", "Estimate_Households_Total_75000_99999", "double"), ("col10", "double", "Estimate_Households_Total_100000_149999", "double"), ("col11", "double", "Estimate_Households_Total_150000_199999", "double"), ("col12", "double", "Estimate_Households_Total_greater_than_200000", "double"), ("col13", "long", "Estimate_Households_Median_income", "long"), ("col14", "long", "Estimate_Households_Mean_income", "long"), ("col15", "double", "Estimate_Households_PERCENT_ALLOCATED_Household_income", "double"), ("col16", "string", "State_Code", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("col0", "string", "County", "string"), ("col1", "string", "State", "string"), ("col2", "long", "Estimate_Households_Total", "long"), ("col3", "double", "Estimate_Households_Total_Less_than_10000", "double"), ("col4", "double", "Estimate_Households_Total_10000_14999", "double"), ("col5", "double", "Estimate_Households_Total_15000_24999", "double"), ("col6", "double", "Estimate_Households_Total_25000_34999", "double"), ("col7", "double", "Estimate_Households_Total_35000_49999", "double"), ("col8", "double", "Estimate_Households_Total_50000_74999", "double"), ("col9", "double", "Estimate_Households_Total_75000_99999", "double"), ("col10", "double", "Estimate_Households_Total_100000_149999", "double"), ("col11", "double", "Estimate_Households_Total_150000_199999", "double"), ("col12", "double", "Estimate_Households_Total_greater_than_200000", "double"), ("col13", "long", "Estimate_Households_Median_income", "long"), ("col14", "long", "Estimate_Households_Mean_income", "long"), ("col15", "double", "Estimate_Households_PERCENT_ALLOCATED_Household_income", "double"), ("col16", "string", "State_Code", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "yelp_redshift_glue", connection_options = {"dbtable": "countywise_average_income_csv", "database": "yelp"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "yelp_redshift_glue", connection_options = {"dbtable": "countywise_average_income_csv", "database": "yelp"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
job.commit()