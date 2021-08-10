# Data Dictionary

# **Dimension Tables**
## Airports Data
 * ident: string (nullable = true) - Airport id
 * type: string (nullable = true) - Airport type, based on size or purpose
 * name: string (nullable = true) - Airport name
 * elevation_ft: float (nullable = true) - Elevation in feet
 * continent: string (nullable = true) - Continent
 * iso_country: string (nullable = true) - Country (ISO-2)
 * iso_region: string (nullable = true) - Region (ISO-2)
 * municipality: string (nullable = true) - Municipality
 * gps_code: string (nullable = true) - Gps code
 * iata_code: string (nullable = true) - IATA code
 * local_code: string (nullable = true) - local code
 * coordinates: string (nullable = true) - Coordinates
 
## U.S. Demographic by State
 * state: string (nullable = true) - Full state name
 * state_code: string (nullable = true)- State code
 * total_population: double (nullable = true) - Total population of the state
 * male_population: double (nullable = true) - Total Male population per state
 * female_population: double (nullable = true) - Total Female population per state
 * foreign_born: double (nullable = true) - number of foreign born people per state
 * white: long (nullable = true) - Total White population per state 
 * asian: long (nullable = true) - Total Asian population per state
 * american_indian_or_alaskan: long (nullable = true) - Total American Indian and Alaska Native population per state
 * hispanic_or_latino: long (nullable = true) - Total Hispanic or Latino population per state 
 * black_or_african_american: long (nullable = true) - Total Black or African-American population per state
 

## Airlines
 * iata_code: string (nullable = true) - IATA code
 * icao_code: string (nullable = true) - ICAO code
 * airline: string (nullable = true) -  Airline name
 * country: string (nullable = true) - Origin country

## Countries
 * country_code: long (nullable = true) - Country code
 * country: string (nullable = true) - Country name

## Ports
 * port_code: long (nullable = true) - Country code
 * port: string (nullable = true) - Country name

## Visas
 * visa_code: string (nullable = true) - Visa code
 * visa_type: string (nullable = true) - Visa description
 
## Mode to access
 * mode_code: integer (nullable = true) - Mode code
 * mode: string (nullable = true) - Mode description
 
# Fact Table (Inmigration Registry)
 * cic_id: integer (nullable = true) - CIC id
 * port_code: string (nullable = true) - Airport code
 * state_code: string (nullable = true) - US State code
 * visa_code: integer (nullable = true) - Visa code
 * mode_code: integer (nullable = true) - Mode code
 * origin_country_code: integer (nullable = true) - Country of origin code
 * origin_city_code: integer (nullable = true) - City code of origin
 * arrival_date: date (nullable = true) - Arrival date
 * arrival_year: integer (nullable = true) - Arrival year
 * arrival_month: integer (nullable = true) - Arrival month
 * arrival_day: integer (nullable = true) - Arrival day
 * departure_date: date (nullable = true) - Departure Date
 * matflag: string (nullable = true) - Match flag - Match of arrival and departure records
 * visapost: string (nullable = true) - Department of State where where Visa was issued
 * dtaddto: string (nullable = true) -  Date to which admitted to U.S. (allowed to stay until)
 * biryear: integer (nullable = true) - Year of Birth
 * gender: string (nullable = true) - Gender
 * airline: string (nullable = true) - Airline code
 * admnum: double (nullable = true) - Admission Number
 * occup: string (nullable = true) - Occupation
 * fltno: string (nullable = true) - Number of flight of arrival to U.S.
 * visatype: string (nullable = true) - Class of admission legally admitting the non-immigrant to temporarily stay in U.S
 
 