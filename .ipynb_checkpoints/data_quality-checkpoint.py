from pyspark.sql.functions import *

class DataValidator:
    """
    Validate and check the model and data.
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def _get_dim_demographics(self):
        """
        Get demographics dimension
        :return: demographics dimension
        """
        return self.spark.read.parquet(self.paths["demographics"])

    def _get_dim_countries(self):
        """
        Get countries dimension
        :return: countries dimension
        """
        return self.spark.read.parquet(self.paths["countries"])

    def _get_dim_visa(self):
        """
        Get visa dimension
        :return: visa dimension
        """
        return self.spark.read.parquet(self.paths["visa"])

    def _get_dim_mode(self):
        """
        Get mode dimension
        :return: mode dimension
        """
        return self.spark.read.parquet(self.paths["modes"])
    
    def _get_dim_airports(self):
        """
        Get airports dimension
        :return: airports dimension
        """
        return self.spark.read.parquet(self.paths["airports"])

    def _get_dim_airlines(self):
        """
        Get airlines dimension
        :return: airlines dimension
        """
        return self.spark.read.parquet(self.paths["airlines"])
    
    def get_facts(self):
        """
        Get facts table
        :return: facts table
        """
        return self.spark.read.parquet(self.paths["facts"])

    def get_dimensions(self):
        """
        Get all dimensions of the model
        :return: all dimensions
        """
        return self._get_dim_demographics(), self._get_dim_countries(), self._get_dim_visa(), \
               self._get_dim_mode(), self._get_dim_airports(), self._get_dim_airlines()
                

    def check_rows_exist(self, spark_df):
        """
        Checks if there is any data in a dataframe
        :param spark_df: spark dataframe
        :return: True if the dataset has any row, False otherwise
        """
        return spark_df.limit(1).count() > 0
    
    def check_unique_records(self, spark_df):
        """
        Checks if all records are unique (no duplicates)
        :param spark_df: spark dataframe
        :return: True if the dataset doesn't contain duplicates, False otherwise
        """
        return spark_df.count() == spark_df.distinct().count() 

    def check_integrity(self, fact, dim_demographics, dim_countries, dim_visa, dim_mode, dim_airports, dim_airlines):
        """
        Check the integrity of the model. Checks if all the facts columns joined with the dimensions has correct values 
        :param fact: fact table
        :param dim_demographics: demographics dimension
        :param dim_countries: countries dimension
        :param dim_visa: visa dimension
        :param dim_mode: mode dimension
        :param dim_airports: airports dimension
        :param dim_airlines: airlines dimension
        :return: true if integrity check is correct, false otherwise
        """
        
        integrity_demo = fact.select(col("state_code")).distinct() \
                             .join(dim_demographics, fact["state_code"] == dim_demographics["state_code"], "left_anti") \
                             .count() == 0
        
        integrity_countries = fact.select(col("origin_country_code")).distinct() \
                                  .join(dim_countries, fact["origin_country_code"] == dim_countries["country_code"], "left_anti") \
                                  .count() == 0

        integrity_visa = fact.select(col("visa_code")).distinct() \
                             .join(dim_visa, fact["visa_code"] == dim_visa["visa_code"], "left_anti") \
                             .count() == 0

        integrity_mode = fact.select(col("mode_code")).distinct() \
                             .join(dim_mode, fact["mode_code"] == dim_mode["mode_code"], "left_anti") \
                             .count() == 0
        
        integrity_airports = fact.select(col("port_code")).distinct() \
                                 .join(dim_airports, fact["port_code"] == dim_airports["local_code"], "left_anti") \
                                 .count() == 0
        
        integrity_airlines = fact.select(col("airline")).distinct() \
                                 .join(dim_airlines, fact["airline"] == dim_airlines["iata_code"], "left_anti") \
                                 .count() == 0

        
        return integrity_demo  & integrity_countries & integrity_visa & \
               integrity_mode & integrity_airports & integrity_airlines