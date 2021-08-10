from pyspark.sql.functions import *

class DataCleaner:
    """
    Cleans the data by filtering, handling missing values, extracting information and assigning proper data types
    """
    
    @staticmethod
    def i94_code_mapper(f_content, extract_data_type, output_filename, output_file_headers, delimiter):
        """
        Clean temperature dataset filtering only records that doesn't have missing temperature.
        Round temperature values to 2 decimal places.
        :param input_file_content: content of the input file
        :param extract_data_type: type of data to extract
        :param output_filename: name of the output file
        :param output_file_headers: output file headers
        :param delimiter: delimiter of specific data section
        """
        
        f_content = f_content[f_content.index(extract_data_type):]
        f_content = f_content[:f_content.index(delimiter)].split('\n')
        f_content = [i.replace("'", "") for i in f_content]
        records = [i.split('=') for i in f_content[1:]]
        with open(output_filename, 'w+') as of:
            of.write(f'{output_file_headers[0]}\t{output_file_headers[1]}\n')
            for rec in records:
                if len(rec) == 2:
                    of.write(f'{rec[0].strip()}\t{"".join(rec[1:]).strip()}\n')
            of.close()

    @staticmethod
    def clean_airports(airports_df):
        """
        Clean airports dataset filtering only US airports and including only airports that are suited for passenger flights.
        Extract iso regions.
        Cast elevation_ft as float.
        :param airports_df: airports spark dataframe
        :return: cleaned airports spark dataframe
        """
        airports_df = airports_df.filter((col("iso_country") == "US") & (col("type").isin("large_airport", "medium_airport", "small_airport"))) \
                                    .withColumn("iso_region", substring(col("iso_region"), 4, 2)) \
                                    .withColumn("elevation_ft", col("elevation_ft").cast("float"))
        
        return airports_df
    
    @staticmethod
    def clean_immigration(immigration_df):
        """
        Clean immigration dataset by applying correct data types - converting to int and date types.
        arrdate and depdate columns are stored in sas date format which requires adding specified number of days to the base sas date (1960-01-01)
        :param immigration_df: airports spark dataframe
        :return: cleaned immigration spark dataframe
        """
        immigration_df = immigration_df.select(col('cicid'), col('i94yr'), col('i94mon'), col('i94cit'), col('i94res'), col('i94port'), 
                                               col('arrdate'), col('i94mode'), col('i94addr'), col('depdate'), col('i94bir'), 
                                               col('i94visa'), col('dtadfile'), col('visapost'), col('occup'), col('matflag'), 
                                               col('biryear'), col('dtaddto'), col('gender'), col('airline'), col('admnum'), 
                                               col('fltno'), col('visatype')) \
                                        .withColumn("cic_id", col("cicid").cast("int")).drop("cicid") \
                                        .withColumn("i94yr", col("i94yr").cast("int")) \
                                        .withColumn("i94mon", col("i94mon").cast("int")) \
                                        .withColumn("origin_city_code", col("i94cit").cast("int")).drop("i94cit") \
                                        .withColumn("origin_country_code", col("i94res").cast("int")).drop("i94res") \
                                        .withColumn("i94mode", col("i94mode").cast("int")) \
                                        .withColumn("biryear", col("biryear").cast("int")) \
                                        .withColumn("i94bir", col("i94bir").cast("int")) \
                                        .withColumn("i94visa", col("i94visa").cast("int")) \
                                        .withColumn("dtadfile", to_date("dtadfile", "yyyyMMdd")) \
                                        .withColumn("arrival_date", expr("date_add('1960-01-01', arrdate)")).drop("arrdate") \
                                        .withColumn("departure_date", expr("date_add('1960-01-01', depdate)")).drop("depdate") \
                                        .withColumn("arrival_day", dayofmonth(col("arrival_date")).cast("int")) \
                                        .withColumnRenamed("i94addr", "state_code") \
                                        .withColumnRenamed("i94port", "port_code") \
                                        .withColumnRenamed("i94yr", "arrival_year") \
                                        .withColumnRenamed("i94mon", "arrival_month") \
                                        .withColumnRenamed("i94visa", "visa_code") \
                                        .withColumnRenamed("i94mode", "mode_code")
        
        return immigration_df
    
    @staticmethod
    def clean_country_codes(countries_df):
        """
        Clean country codes dataset by removing invalid records
        :param countries_df: spark dataframe containing country codes and descriptions
        :return: cleaned country codes spark dataframe
        """
        countries_df = countries_df.filter((~lower(col("country")).contains("invalid")) & \
                                           (~lower(col("country")).contains("no country code")) & \
                                           (~lower(col("country")).contains("collapsed")))
    
        return countries_df
    
    @staticmethod
    def clean_port_codes(ports_df):
        """
        Clean country codes dataset by removing invalid records and splitting port information into city and state columns
        :param ports_df: spark dataframe containing port codes and names
        :return: cleaned port codes spark dataframe
        """
        ports_df = ports_df.filter((~lower(col("port")).contains("collapsed")) & \
                                   (~lower(col("port")).contains("no port code"))) \
                           .withColumn("port_city", initcap(split(col("port"), ",").getItem(0))) \
                           .withColumn("port_state", split(col("port"), ",").getItem(1)).drop("port") \
        
        return ports_df
    
    @staticmethod
    def clean_airlines(airlines_df):
        """
        Clean country codes dataset by removing invalid records and splitting port information into city and state columns
        :param airlines_df: spark dataframe containing port codes and names
        :return: cleaned airlines spark dataframe
        """
        airlines_df = airlines_df.withColumn("country", regexp_extract("airline", "\((.+)\)", 1))

        return airlines_df
    
    @staticmethod
    def clean_demographics(demographics_df):
        
        demographics_df = demographics_df.groupBy(col("City"), col("State"), col("Median Age"), col("Male Population"), \
                                                 col("Female Population"), col("Total Population"), col("Number of Veterans"), \
                                                 col("Foreign-born"), col("Average Household Size"), col("State Code")) \
                                         .pivot("Race").agg(sum("Count").cast("integer")) \
                                         .fillna({"White": 0,
                                                  "Asian": 0,
                                                  "American Indian and Alaska Native": 0,
                                                  "Hispanic or Latino": 0,
                                                  "Black or African-American": 0})
        
        return demographics_df