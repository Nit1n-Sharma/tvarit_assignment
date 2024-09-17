from pyspark.sql.types import *
from pyspark.sql.functions import *
from resources.dev.config import *
from src.main.utility.spark_session import *

#                                  CREATING SPARK SESSION
logger.info("************* Creating spark session **********")
spark = spark_session()
logger.info("*************** Spark session created**********")

#                           Load school-level data
school_level_df = spark.read.csv(
    BrazilEduPanel_Municipal_csv, header=True, inferSchema=True
)


#                           Load municipal-level data


municipal_level_df = spark.read.csv(
    BrazilEduPanel_School_csv, header=True, inferSchema=True
)

logger.info("*************** DATAFRAME CREATED ************")


"""
                        FILLING NULL VALUES FOR  NU_ANO_CENSO
"""
school_level_df = school_level_df.fillna({"NU_ANO_CENSO": 2021})
municipal_level_df = municipal_level_df.fillna({"NU_ANO_CENSO": 2021})

logger.info("****************** NULL VALUES FILLED SUCCESSFULLY ****************")

"""    
                                    Drop duplicates   
"""
school_level_df = school_level_df.dropDuplicates(
    ["CO_MUNICIPIO", "MUNIC", "NU_ANO_CENSO"]
)
municipal_level_df = municipal_level_df.dropDuplicates(
    ["CO_ENTIDADE", "NU_ANO_CENSO", "MUNIC"]
)

logger.info("******************** DUPLICATE DROPPED **********************")

#           Partition school-level data by state code (CO_UF)
school_level_df_partitioned = school_level_df.repartition("CO_UF")

#           Partition municipal-level data by municipality code (CO_MUNICIPIO)

municipal_level_df_partitioned = municipal_level_df.repartition("CO_MUNICIPIO")
logger.info("************** DATA PARTITIONED SUCCESSFULLY*************")


# Cache the partitioned data for faster access during multiple transformations
school_level_df_partitioned.cache()
municipal_level_df_partitioned.cache()

logger.info("************** DATA CACHED SUCCESSFULLY *************")


# Forward fill missing MATFUND_NU values
logger.info("*************** Handling missing enrolments with forward fill **********")
window_spec = Window.partitionBy("CO_MUNICIPIO").orderBy("NU_ANO_CENSO")

school_level_df_partitioned = school_level_df_partitioned.withColumn(
    "MATFUND_NU", last("MATFUND_NU", ignorenulls=True).over(window_spec)
)

logger.info("*************** Missing enrolments handled **********")


#               Filter for 2021 and aggregate total enrolments by state

enrolments_by_state_2021 = (
    school_level_df_partitioned.filter(col("NU_ANO_CENSO") == 2021)
    .groupBy("CO_UF")
    .agg(sum("MATFUND_NU").alias("Total_Enrolments"))
    .orderBy(col("Total_Enrolments"), ascending=False)
)

enrolments_by_state_2021.show()
logger.info("************ AGGREGATION BY STATE COMPLETED ************")

# Save enrolments by state to MySQL
enrolments_by_state_2021.write.jdbc(
    url=db_url, table="EnrolmentsByState", mode="overwrite", properties=db_properties
)


#                       FILTER FOR 2021 AND COMPUTE TOTAL ENROLMENTS BY CITY
top_10_cities_2021 = (
    school_level_df_partitioned.filter(col("NU_ANO_CENSO") == 2015)
    .groupBy("CO_MUNICIPIO")
    .agg(sum("MATFUND_NU").alias("Total_Enrolments"))
    .orderBy(col("Total_Enrolments"), ascending=False)
    .limit(10)
)
logger.info("************ TOP 10 CITIES AGGREGATION COMPLETED ************")
top_10_cities_2021.show()
# Save top 10 cities to MySQL
top_10_cities_2021.write.jdbc(
    url=db_url, table="Top10Cities", mode="overwrite", properties=db_properties
)


#           Filter data for the 10-year period and aggregate enrolments by year, city, and state

enrolments_by_year_city_state = (
    school_level_df_partitioned.filter(col("NU_ANO_CENSO").between(2012, 2021))
    .groupBy("NU_ANO_CENSO", "CO_UF", "CO_MUNICIPIO")
    .agg(sum("MATFUND_NU").alias("Total_Enrolments"))
    .orderBy("NU_ANO_CENSO", "CO_UF", "CO_MUNICIPIO")
)
enrolments_by_year_city_state.show()
logger.info("************ AGGREGATION BY YEAR, CITY, AND STATE COMPLETED ************")
# Save enrolments by year, city, and state to MySQL
enrolments_by_year_city_state.write.jdbc(
    url=db_url,
    table="EnrolmentsByYearCityState",
    mode="overwrite",
    properties=db_properties,
)


logger.info("************ DATA SAVED TO MYSQL SUCCESSFULLY ************")


# Set batch size for better performance when saving to MySQL
db_properties["batchsize"] = "10000"
