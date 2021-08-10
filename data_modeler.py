from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *


class DataModeler:
    """
    Models the star schema data warehouse from datasets. Creates the fact table and dimension tables and stores them in parquet format format.
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths
    
    def _model_dim_demographics(self, demographics):
        """
        Create demographics dimension table in parquet format.
        :param demographics: demographics dataset.
        """
        demographics.write.mode("overwrite").parquet(self.paths["demographics"])
    
    def _model_dim_airports(self, airports):
        """
        Create airports dimension table in parquet format.
        :param airports: airports dataset
        """
        airports.write.mode("overwrite").parquet(self.paths["airports"])
    
    def _model_dim_countries(self, countries):
        """
        Create countries dimension table in parquet format.
        :param countries: countries dataset
        """
        countries.write.mode("overwrite").parquet(self.paths["countries"])

    def _model_dim_visa(self, visa):
        """
        Create visa dimension table in parquet format.
        :param visa: visa dataset
        """
        visa.write.mode("overwrite").parquet(self.paths["visa"])

    def _model_dim_modes(self, modes):
        """
        Create modes dimension table in parquet format.
        :param mode: modes dataset
        """
        modes.write.mode("overwrite").parquet(self.paths["modes"])
        
    def _model_dim_airlines(self, airlines):
        """
        Create airlines dimension table in parquet format.
        :param airlines: airlines dataset
        """
        airlines.write.mode("overwrite").parquet(self.paths["airlines"])

    def _model_fact_immigration(self, facts):
        """
        Create fact table from immigration in parquet format partitioned by arrival_year, arrival_month and arrival_day.
        :param facts: immigration dataset
        """
        facts.write.partitionBy("arrival_year", "arrival_month", "arrival_day") \
             .mode("overwrite").parquet(self.paths["facts"])
    
    
    def model_dwh(self, fact_immigration, dim_demographics, dim_countries, dim_visa, dim_mode, dim_airports, dim_airlines):
        """
        Create the Star Schema for the Data Warwhouse
        :param fact_immigration: immigration facts table
        :param dim_demographics: demographics dimension
        :param dim_countries: countries dimension
        :param dim_visa: visa dimension
        :param dim_mode: modes dimension
        :param dim_airports: airports dimension
        :param dim_airlines: airlines dimension
        """
        fact_immigration = fact_immigration \
            .join(dim_demographics, fact_immigration["state_code"] == dim_demographics["state_code"], "left_semi") \
            .join(dim_countries, fact_immigration["origin_country_code"] == dim_countries["country_code"], "left_semi") \
            .join(dim_visa, fact_immigration["visa_code"] == dim_visa["visa_code"], "left_semi") \
            .join(dim_mode, fact_immigration["mode_code"] == dim_mode["mode_code"], "left_semi") \
            .join(dim_airports, fact_immigration["port_code"] == dim_airports["local_code"], "left_semi") \
            .join(dim_airlines, fact_immigration["airline"] == dim_airlines["iata_code"], "left_semi") 
        
        self._model_dim_demographics(dim_demographics)
        self._model_dim_countries(dim_countries)
        self._model_dim_visa(dim_visa)
        self._model_dim_modes(dim_mode)
        self._model_dim_airports(dim_airports)
        self._model_dim_airlines(dim_airlines)
        
        self._model_fact_immigration(fact_immigration)