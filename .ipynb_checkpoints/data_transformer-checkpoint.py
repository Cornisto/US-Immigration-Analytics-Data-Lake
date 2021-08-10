from pyspark.sql.functions import *

class DataTransformer:
    """
    Performs necessary transformations on the datasets to enable modelling data warehouse 
    """
    
    @staticmethod
    def transform_demographics(demographics_df):
        """
        Transform demographics data by grouping by state
        :param demographics_df: demographics dataset
        :return: demographics_df dataset transformed
        """
        
        demographics_df = demographics_df.groupBy(col("State Code").alias("state_code"), \
                                                  col("State").alias("state")) \
                                         .agg(sum("Total Population").cast("int").alias("total_population"), \
                                              sum("Male Population").cast("int").alias("male_population"), \
                                              sum("Female Population").cast("int").alias("female_population"), \
                                              sum("Foreign-born").cast("int").alias("foreign_born"), \
                                              sum("White").alias("white"), \
                                              sum("Asian").alias("asian"), \
                                              sum("Hispanic or Latino").alias("hispanic_or_latino"), \
                                              sum("Black or African-American").alias("black_or_african_american"), \
                                              sum("American Indian and Alaska Native").alias("american_indian_or_alaskan"))
                                              
        return demographics_df