import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType
from pyspark.sql.functions import col, sum, udf, when, round
from pyspark.sql.functions import monotonically_increasing_id


def create_spark_session():
    print('*** CREATING SPARK SESSION ***')
    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark


def process_i94_data(spark, output_data):
    '''function to load, clean and write the data from i94 dataset to the fact table fact_i94'''

    # read i94 data file
    print('*** READING i94 DATA ***')
    df_i94 = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat')

    # clean data
    print('*** PROCESSIONG i94 DATA ***')
    # function to convert date column into a date format
    @udf(DateType())
    def convert_to_date(date):
        if date is not None:
            return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')

    # convert dates to datetime object    
    df_i94 = df_i94.withColumn('arrdate', convert_to_date(col('arrdate')))
    df_i94 = df_i94.withColumn('depdate', convert_to_date(col('depdate')))

    # convert columns to integer
    df_i94 = df_i94.withColumn("i94bir", round(df_i94["i94bir"]).cast('integer'))
    df_i94 = df_i94.withColumn("i94yr", round(df_i94["i94yr"]).cast('integer'))
    df_i94 = df_i94.withColumn("i94mon", round(df_i94["i94mon"]).cast('integer'))
    df_i94 = df_i94.withColumn("i94visa", round(df_i94["i94visa"]).cast('integer'))
    df_i94 = df_i94.withColumn("i94mode", round(df_i94["i94mode"]).cast('integer'))
    df_i94 = df_i94.withColumn("i94cit", round(df_i94["i94cit"]).cast('integer'))
    df_i94 = df_i94.withColumn("i94res", round(df_i94["i94res"]).cast('integer'))

    # create fact table and drop duplicates
    fact_i94 = df_i94.withColumn('id', monotonically_increasing_id())\
        .select('id', 'cicid', 'arrdate', 'i94yr', 'i94mon', 'depdate', 'i94port', 'i94addr', 'i94mode'\
                , 'i94cit', 'i94res', 'i94visa', 'visatype', 'i94bir', 'gender', 'insnum', 'admnum').drop_duplicates()   

    # write fact_i94 table to parquet files partitioned by year and month
    print('*** WRITING i94 DATA ***')
    fact_i94.write \
        .partitionBy('i94yr', 'i94mon') \
        .mode('overwrite') \
        .parquet('{}fact_i94/'.format(output_data))
    
    
def process_i94_description_data(spark, output_data):
    print('*** PROCESSING i94 DESCRIPTION DATA ***')
    # read SAS file
    with open('raw_data/I94_SAS_Labels_Descriptions.SAS') as file:
        I94_descriptions = file.readlines()
        
    print('*** PROCESSING AND WRITING DIM_COUNTRY ***')
    # extract country info: used in i94CIT and i94RES - '236': 'AFGHANISTAN'
    country_codes = {}
    for line in I94_descriptions[10:298]:
        line = line.split('=')
        country_codes[line[0].strip()] = line[1].strip().strip("'")
    dim_country = pd.DataFrame.from_dict(country_codes, orient="index").reset_index()
    dim_country.columns=['country_code', 'country_name']
    # convert pandas df to spark df in order to save it as parquet
    dim_country=spark.createDataFrame(dim_country) 
    dim_country.write \
        .mode('overwrite') \
        .parquet('{}dim_country/'.format(output_data))
        
    print('*** PROCESSING AND WRITING DIM_CITY ***')
    # extract port info: used in i94PORT (airport or harbour info) - 'ANC': 'ANCHORAGE, AK'
    port_codes = {}
    for line in I94_descriptions[303:962]:
        line = line.split('=')
        port_codes[line[0].strip().strip("'")] = line[1].strip().strip("'").strip()
    dim_city = pd.DataFrame.from_dict(port_codes, orient="index").reset_index()
    dim_city.columns=['city_code', 'city']
    # convert pandas df to spark df in order to save it as parquet
    dim_city=spark.createDataFrame(dim_city) 
    dim_city.write \
        .mode('overwrite') \
        .parquet('{}dim_city/'.format(output_data))
        
    print('*** PROCESSING AND WRITING DIM_STATE ***')
    # extract state info: used in i94ADDR - 'AK': 'ALASKA'
    state_codes = {}
    for line in I94_descriptions[982:1036]:
        line = line.split('=')
        state_codes[line[0].strip().strip("'")] = line[1].strip().strip("'")
    dim_state = pd.DataFrame.from_dict(state_codes, orient="index").reset_index()
    dim_state.columns=['state', 'state_name']
    # convert pandas df to spark df in order to save it as parquet
    dim_state=spark.createDataFrame(dim_state) 
    dim_state.write \
        .mode('overwrite') \
        .parquet('{}dim_state/'.format(output_data))

        
    print('*** PROCESSING AND WRITING DIM_TRAVELMODE ***')
    # extract travelmode info: used in i94mode - '1': 'AIR'
    travelmode_codes = {}
    for line in I94_descriptions[972:976]:
        line = line.split('=')
        travelmode_codes[int(line[0].strip())] = line[1].strip().strip("'")
    dim_travelmode = pd.DataFrame.from_dict(travelmode_codes, orient="index").reset_index()
    dim_travelmode.columns=['code', 'travelmode']
    # convert pandas df to spark df in order to save it as parquet
    dim_travelmode=spark.createDataFrame(dim_travelmode) 
    dim_travelmode.write \
        .mode('overwrite') \
        .parquet('{}dim_travelmode/'.format(output_data))


    print('*** PROCESSING AND WRITING DIM_VISACAT ***')
    # extract visacategory info: used in i94visa - '1': 'Business'
    visacat_codes = {}
    for line in I94_descriptions[1046:1049]:
        line = line.split('=')
        visacat_codes[int(line[0].strip())] = line[1].strip()#.strip("'")
    dim_visacat = pd.DataFrame.from_dict(visacat_codes, orient="index").reset_index()
    dim_visacat.columns=['code', 'visacat']
    # convert pandas df to spark df in order to save it as parquet
    dim_visacat=spark.createDataFrame(dim_visacat) 
    dim_visacat.write \
        .mode('overwrite') \
        .parquet('{}dim_visacat/'.format(output_data))
    
    
def process_city_data(spark, output_data):
    print('*** PROCESSING DEMOGRAPHICS DATA ***')
    # US Cities Demographics
    df_cities = pd.read_csv('raw_data/us-cities-demographics.csv', delimiter=';')
    # select needed columns and drop duplicate rows
    dim_demographics = df_cities[['City', 'State', 'Median Age', 'Average Household Size'\
                                  , 'Total Population', 'Female Population', 'Male Population', 'Foreign-born']]
    dim_demographics = dim_demographics.drop_duplicates()
    # rename columns
    new_columns = ['City_name', 'State_name', 'Median_Age', 'Average_Household_Size'\
                                  , 'Total_Population', 'Female_Population', 'Male_Population', 'Foreign_born']
    dim_demographics.columns = new_columns
    
    print('*** WRITING DEMOGRAPHICS DATA ***')
    dim_demographics=spark.createDataFrame(dim_demographics) 
    dim_demographics.write \
        .mode('overwrite') \
        .parquet('{}dim_demographics/'.format(output_data))


    
def main():
    print('*** STARTING ETL PROCESS ***\n')

    spark = create_spark_session()
    output_data = "data_store/"

    process_i94_data(spark, output_data)
    process_i94_description_data(spark, output_data)
    process_city_data(spark, output_data)

    print('\n*** ETL PROCESS COMPLETE ***')

if __name__ == "__main__":
    main()
