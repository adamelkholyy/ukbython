import os
import pyspark
import subprocess
import json
import time
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# pdoc ./ukbython -o ./docs

class ukbython:
    """
    ukbython: Tools for easily accessing UK Biobank RAP data. Features include simple filtering, fast phenotype retrieval, and custom SQL querying. For a similar package in R see Luke Pilling's ukbrapR: https://github.com/lcpilling/ukbrapR/blob/main/R/get_diagnoses.R .
    """

    spark: pyspark.sql.SparkSession
    """The PySpark session instance."""
    database: str
    """The name of the UK Biobank database in use."""

    """ 
    Initialises the ukbython instance, which will allow us to access Biobank data through the ```spark``` variable. Sets up the PySpark session, finds and links up the relevant UK Biobank database. 
    """
    def __init__(self):
        #TODO: Lock for PySpark
        config = pyspark.SparkConf().setAll([('spark.kryoserializer.buffer.max', '128'),('spark.sql.execution.arrow.pyspark.enabled','true')])  
        sc = pyspark.SparkContext(conf=config)
        self.spark = pyspark.sql.SparkSession(sc)
        self.database = None
        self.get_database_from_nexus()



    def get_icd9(self, codes: list, unique_eids=True):
        """
        Returns a DataFrame of all eids and the first diagnosis date of matching ICD9 records.

        **Args**
        - `codes` (`list`): List of ICD9 codes for filtering
        - `unique_eids` (`bool`): If True, return only unique eids with the earliest diagnosis date for each

        **Returns**
        - `DataFrame`: Containing all matching eids and earliest diagnosis date for given ICD9 codes
        """
        start = time.time()
        print(f"Searching {len(codes)} ICD9 codes...")
        
        # ICD9: remove the . from codes, first 5 characters only
        formatted_codes = ", ".join([f"'{code.replace('.', '')[:5]}'" for code in codes])
        
        df = self.spark.sql(f"SELECT DISTINCT eid, dnx_hesin_id FROM `{self.database}`.`hesin_diag` WHERE diag_icd9 IN ({formatted_codes})")
        df = self.get_hesin_dates(df, earliest_date=unique_eids)
        print(f"Operation complete: Found {df.count()} matching records in {(time.time() - start):.2f} seconds.")
        
        return df


    def get_icd10(self, codes:list, unique_eids=True):
        """
        Returns a DataFrame of all eids and the first diagnosis date of matching ICD10 records across HESIN, Death, and Cancer records.

        **Args**
        - `codes` (`list`): List of ICD10 codes for filtering
        - `unique_eids` (`bool`): If True, return only unique eids with the earliest diagnosis date for each

        **Returns**
        - `DataFrame`: Containing all matching eids and earliest diagnosis date for given ICD10 codes
        """
        start = time.time()
        print(f"Searching {len(codes)} ICD10 codes...")
        hesin_df = self.get_icd10_hesin(codes, earliest_date=unique_eids)
        print(f"Found {hesin_df.count()} matching hospital records")

        death_df = self.get_icd10_death(codes)
        print(f"Found {death_df.count()} matching death records")

        cancer_df = self.get_icd10_cancer(codes, unique_eids=unique_eids)
        print(f"Found {cancer_df.count()} matching cancer records")

        # join dfs together
        df = hesin_df.join(death_df, on="eid", how="outer")
        df = df.join(cancer_df, on="eid", how="outer")

        # get earliest date of diagnosis if unique_eids == True
        if unique_eids:
            df = df.withColumn(
                "diagnosis_date",
                F.least(
                    F.col("diagnosis_date"),
                    F.col("cancer_date"),
                    F.col("date_of_death")
                )
            )
            df = df.select("eid", "diagnosis_date")
            
        print(f"Operation complete: Found {df.count()} matching records in {(time.time() - start):.2f} seconds.")
        return df


    def get_icd10_hesin(self, codes: list, unique_eids=True):
        """
        Returns a DataFrame of all eids and the first diagnosis date of matching ICD10 diagnoses from hospital records.

        **Args**
        - `codes` (`list`): List of ICD10 codes for filtering
        - `unique_eids` (`bool`): If True, return only unique eids with the earliest diagnosis date for each

        **Returns**
        - `DataFrame`: Containing all matching eids and earliest diagnosis date for given ICD10 codes
        """
                
        # ICD10: remove . from codes, first 5 characters only
        formatted_codes = ", ".join([f"'{code.replace('.', '')[:5]}'" for code in codes])
        df = self.spark.sql(f"SELECT DISTINCT eid, dnx_hesin_id FROM `{self.database}`.`hesin_diag` WHERE diag_icd10 IN ({formatted_codes})")
        df = self.get_hesin_dates(df, unique_eids=unique_eids)
        return df
    
    
    def get_icd10_death(self, codes: list):
        """
        Returns a DataFrame of all eids and the first diagnosis date of matching ICD10 causes from death records.

        **Args**
        - `codes` (`list`): List of ICD10 codes for filtering

        **Returns**
        - `DataFrame`: Containing all matching eids and death date for given ICD10 codes
        """

        # ICD10: remove . from codes, first 5 characters only
        formatted_codes = ", ".join([f"'{code.replace('.', '')[:5]}'" for code in codes])
        # DISTINCT is perfunctory here ... you only die once!
        df = self.spark.sql(f"SELECT eid, dnx_death_id FROM `{self.database}`.`death_cause` WHERE cause_icd10 IN ({formatted_codes})")
        df = self.get_death_dates(df)
        return df
    

    def get_icd10_cancer(self, codes: list, unique_eids=True):
        """
        Returns a DataFrame of all eids and the first diagnosis date of matching ICD10 diagnoses from cancer records.

        **Args**
        - `codes` (`list`): List of ICD10 codes for filtering
        - `unique_eids` (`bool`): If True, return only unique eids with the earliest diagnosis date for each

        **Returns**
        - `DataFrame`: Containing all matching eids and earliest diagnosis date for given ICD10 codes
        """

        # ICD10: remove . from codes, first 5 characters only
        formatted_codes = ", ".join([f"'{code.replace('.', '')[:5]}'" for code in codes])

        # generate all cancer fields query
        case_statements = [f"WHEN p40006_i{i} IN ({formatted_codes}) THEN p40005_i{i}" for i in range(22)]
        case_condition = " ".join(case_statements)

        # query all cancer fields and associated dates
        query = f"""
        SELECT DISTINCT eid, 
               CASE {case_condition} 
               ELSE NULL 
               END AS cancer_date
        FROM `{self.database}`.`participant_0073`
        WHERE {" OR ".join([f"p40006_i{i} IN ({formatted_codes})" for i in range(22)])}
        """

        df = self.spark.sql(query)

        # get earliest available date for duplicate eids
        if unique_eids:
            df = df.groupBy("eid").agg(F.min("cancer_date").alias("cancer_date"))
            
        return df
        
    
    def get_opcs(self, codes: list, unique_eids=True):
        """
        Returns a DataFrame of all eids and the first diagnosis date of matching OPCS3/OPCS4 records from hospital operation records.

        **Args**
        - `codes` (`list`): List of OPCS codes for filtering
        - `unique_eids` (`bool`): If True, return only unique eids with the earliest diagnosis date for each

        **Returns**
        - `DataFrame`: Containing all matching eids and earliest diagnosis date for given OPCS codes
        """
        start = time.time()
        print(f"Searching {len(codes)} OPCS3/OPCS4 codes...")
        
        # OPCS3, OPCS4: remove the . from codes, first 5 chars only
        formatted_codes = ", ".join([f"'{code.replace('.', '')[:5]}'" for code in codes])
        df = self.spark.sql(f"SELECT DISTINCT eid, opdate FROM `{self.database}`.`hesin_oper` WHERE oper3 IN ({formatted_codes}) OR oper4 in ({formatted_codes})")
        
        # get earliest available date for duplicate eids
        if unique_eids:
            df = df.groupBy("eid").agg(F.min("opdate").alias("opdate"))
        
        print(f"Operation complete: Found {df.count()} matching records in {(time.time() - start):.2f} seconds.")
        return df

    

    def get_gp_clinical(self, codes: list, unique_eids=True):
        """
        Returns a DataFrame of all eids and the first diagnosis date of matching Read2/Read3 codes from gp clinical records.

        **Args**
        - `codes` (`list`): List of Read2/Read3 codes for filtering
        - `unique_eids` (`bool`): If True, return only unique eids with the earliest diagnosis date for each

        **Returns**
        - `DataFrame`: Containing all matching eids and earliest diagnosis date for given Read2/CTV3 codes
        """
        start = time.time()
        print(f"Searching {len(codes)} Read2/Read3 codes...")
        
        # Read2, Read3: first 5 chars only
        formatted_codes = ", ".join([f"'{code[:5]}'" for code in codes])
        df = self.spark.sql(f"SELECT DISTINCT eid, event_dt FROM `{self.database}`.`gp_clinical` WHERE read_2 IN ({formatted_codes}) OR read_3 IN ({formatted_codes})")
        
        # get earliest available date for duplicate eids
        if unique_eids:
            df = df.groupBy("eid").agg(F.min("event_dt").alias("event_dt"))
        
        print(f"Operation complete: Found {df.count()} matching records in {(time.time() - start):.2f} seconds.")
        return df


    def get_hesin_dates(self, df: DataFrame, earliest_date=True):
        """
        Returns a DataFrame of diagnosis dates of matching eids from hospital inpatient records.

        **Args**
        - `df` (`DataFrame`): DataFrame of eids to retreive diagnosis dates for 
        - `earliest_date` (`bool`): If True, return only the earliest diagnosis date for each eid

        **Returns**
        - `DataFrame`: Containing all diagnosis dates for given eids
        """
        hesin_df = self.spark.sql(f"SELECT dnx_hesin_id, epistart, epiend, admidate, disdate FROM `{self.database}`.`hesin`")

        df = df.join(hesin_df, on="dnx_hesin_id", how="inner")

        # get diagnosis date: takes the first non-null value since fields are in chronological order
        df = df.withColumn('diagnosis_date', F.coalesce('epistart', 'epiend', 'admidate', 'disdate'))

        # group by eid and get the earliest diagnosis date for each participant
        if earliest_date:
            df = df.groupBy('eid').agg(F.min('diagnosis_date').alias('diagnosis_date'))

        return df.select("eid", "diagnosis_date")

    

    def get_death_dates(self, df: DataFrame):
        """
        Returns a DataFrame of death dates of matching eids from death records.

        **Args**
        - `df` (`DataFrame`): DataFrame of eids to retreive death dates for 

        **Returns**
        - `DataFrame`: Containing all diagnosis dates for given eids
        """

        # get death dates and join to eids
        dates_df = self.spark.sql(f"SELECT eid, date_of_death FROM `{self.database}`.`death`")
        df = df.join(dates_df, on="eid", how="inner")
        df = df.select("eid", "date_of_death")

        return df


 
    def get_rap_field(self, codes: list):
        """
        Returns a DataFrame of eids and given RAP fields.

        **Args**
        - `codes` (`list`): List of RAP fields for filtering

        **Returns**
        - `DataFrame`: Containing all database eids and specified RAP fields
        """
        start = time.time()
        print(f"Searching {len(codes)} RAP fields...")

        # validation for field lookup table
        if not os.path.isfile('field_lookup.json'):
            raise FileNotFoundError('field_lookup.json not found... Try running:\n curl -L -o field_lookup.json "https://raw.githubusercontent.com/adamelkholyy/ukbython/main/field_lookup.json"')

        # load the field lookup dictionary for fast lookup of RAP fields
        with open('field_lookup.json', 'r') as json_file:
            lookup_dict = json.load(json_file)
        lookup = [(code, lookup_dict[code]) for code in codes]
        del lookup_dict
        
        # validation for dictionary loading
        if len(lookup) == 0:
            raise ValueError("Fields not found...")
        
        # print the tables where fields are found for user's reference
        for code, table in lookup:
            print(f"{code} found in {table}")
        
        # join given fields together with all eids
        code, table = lookup[0]
        df = self.spark.sql(f"SELECT eid, {code} FROM `{self.database}`.`{table}`")    
        for code, table in lookup[1:]:
            new_df = self.spark.sql(f"SELECT eid, {code} FROM `{self.database}`.`{table}`")
            df = df.join(new_df, on="eid", how="outer")
            
        print(f"Operation complete: Found {df.count()} matching records in {(time.time() - start):.2f} seconds.")
        return df



    def get_database_from_nexus(self):
        """
        Searches the DNANexus directory for all viable database files. Sets the first one found if available, otherwise prompts the user to set the database manually.
        """
        dataset_file = None
        try:
            # execute the `dx ls` command
            result = subprocess.run(
                ["dx", "ls", "/"],
                text=True,
                capture_output=True,
                check=True
            )
            # sets the first .dataset file found as database
            files = result.stdout.splitlines()
            dataset_file = [file[:-8] for file in files if file.endswith('.dataset')][0]

        # error: cannot list databases, list databases manually instead
        except subprocess.CalledProcessError as e:
            self.find_databases()
            raise e(f"Error listing DNA nexus databases: {e.stderr}. Please define database manually from any of the available sets above using set_database(database_name)")

        # dataset file found
        if dataset_file:
            print(f"Running on database {dataset_file}...")
            self.database = dataset_file
        # error: no dataset files found, list databases manually instead
        else:
            self.find_databases()
            raise ValueError(f"No .dataset files found in nexus directory... Please define database manually from any of the available sets above using set_database(database_name)")


    def find_databases(self):
        """
        Lists available databases in the PySpark session.
        """
        try:
            # list databases using PySpark
            databases = [row.namespace for row in self.spark.sql("SHOW DATABASES").collect()]
            print(f"Found {len(databases)} available databases:")
            for i, db in enumerate(databases):
                print(f"{i + 1}. {db}")
        # error: no databases found
        except Exception as e:
            raise e(f"Error finding available databases: {e.stderr}")

        
    def set_database(self, new_database: str):
        """
        Setter method to change the database in use.
        """
        self.database = new_database

        
    def write_df_to_file(self, df: DataFrame, filename: str):
        """
        Returns a DataFrame of eids and given RAP fields.

        **Args**
        - `df` (`DataFrame`): DatFrame to write to file
        - `filename` (`str`): Filename to save DataFrame to, e.g. 'output.csv', 'output.tsv' etc.
        """
        pdf = df.toPandas()
        pdf.to_csv(filename, index=False, encoding="utf-8")
        print(f"{filename} saved successfully")

        
    def __str__(self):
        return f"PySpark running on database '{self.database}'"
