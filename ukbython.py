import pyspark
import subprocess
import json
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# TODO
# comments and docs
# lock for pyspark
# pipeline for all icd10 queries
# pipeline for one query for all coding schemas
# main example 
# hmb_mp_phenotype main conversion to newer ukbython package

# pdoc ./ukbython -o ./docs
# code filtering https://github.com/lcpilling/ukbrapR/blob/main/R/get_diagnoses.R, line: 122



class ukbython:
    """
    ukbython: Tools for easily accessing UK Biobank RAP data. Features include simple filtering, fast phenotype retrieval, and custom SQL querying.
    **Example**:
    ```python
    ukb = ukbython()
    >>> Running on database app31234..."
    icd9= ['12345', '67890']
    df = get_icd9(codes)
    dates = get_icd9_dates(df)
    df.show()
    >>> [example]
    ```
    """

    spark: pyspark.sql.SparkSession
    """The PySpark session instance."""
    database: str
    """The UK Biobank database in use."""

    """ 
    Initialises the ukbython instance, which will allow us to access Biobank data through the ```spark``` variable. Sets up the PySpark session, finds and links up the 
    relevant UK Biobank database. 
    """
    def __init__(self):
        config = pyspark.SparkConf().setAll([('spark.kryoserializer.buffer.max', '128'),('spark.sql.execution.arrow.pyspark.enabled','true')])  
        sc = pyspark.SparkContext(conf=config)
        self.spark = pyspark.sql.SparkSession(sc)
        self.database = None
        self.get_database_from_nexus()




    def example_doc(self, codes: list):
        """
        Example doc text.

        **Arguments**:
        - `codes` (`list`): List of ICD9 codes for filtering.

        **Returns**:
        - `DataFrame`: A DataFrame containing all matching eids for the given ICD9 codes.

        **Raises**:
        - `ValueError`: If the list of codes is empty.

        **Example**:
        ```python
        codes = ['12345', '67890']
        df = get_icd9(codes)
        ```

        This function queries the database for all records that match the provided ICD9 codes and returns a DataFrame with the results.
        """
        pass





    def get_icd9(self, codes: list, unique_eids=True):
        """
        Returns a DataFrame of all eids with matching ICD9 records

        Args:   
        - codes (list): List of ICD9 codes for filtering

        Returns:
            DataFrame: Containing all matching eids for given ICD9 codes
        """
        distinct = "DISTINCT" if unique_eids else ""
        # ICD9: remove . from codes, first 5 characters only
        formatted_codes = ", ".join([f"'{code.replace('.', '')[:5]}'" for code in codes])
        df = self.spark.sql(f"SELECT {distinct} eid, dnx_hesin_id FROM `{self.database}`.`hesin_diag` WHERE diag_icd9 IN ({formatted_codes})")
        return df
    



    def get_icd10_hesin(self, codes: list, unique_eids=True):
        distinct = "DISTINCT" if unique_eids else ""
        # ICD10: remove . from codes, first 5 characters only
        formatted_codes = ", ".join([f"'{code.replace('.', '')[:5]}'" for code in codes])
        df = self.spark.sql(f"SELECT {distinct} eid, dnx_hesin_id FROM `{self.database}`.`hesin_diag` WHERE diag_icd10 IN ({formatted_codes})")
        return df
    
    
    def get_icd10_death(self, codes: list):
        # remove the . from codes, first 5 characters only 
        formatted_codes = ", ".join([f"'{code.replace('.', '')[:5]}'" for code in codes])
        # DISTINCT is perfunctory here ... you only die once!
        df = self.spark.sql(f"SELECT eid, dnx_death_id FROM `{self.database}`.`death_cause` WHERE cause_icd10 IN ({formatted_codes})")
        return df
    
    
    
    def get_hesin_oper(self, codes: list, unique_eids=True, earliest_date=True):
        distinct = "DISTINCT" if unique_eids else ""
        # OPCS3, OPCS4: remove the . from codes, first 5 chars only
        formatted_codes = ", ".join([f"'{code.replace('.', '')[:5]}'" for code in codes])
        df = self.spark.sql(f"SELECT {distinct} eid, opdate FROM `{self.database}`.`hesin_oper` WHERE oper3 IN ({formatted_codes}) OR oper4 in ({formatted_codes})")
        
        # get earliest available date for duplicate eids
        if earliest_date:
            df = df.groupBy("eid").agg(F.min("opdate").alias("opdate"))
            
        return df

    

    def get_gp_clinical(self, codes, unique_eids=True, earliest_date=True):
        distinct = "DISTINCT" if unique_eids else ""
        # Read2, Read3: first 5 chars only
        formatted_codes = ", ".join([f"'{code[:5]}'" for code in codes])
        df = self.spark.sql(f"SELECT {distinct} eid, event_dt FROM `{self.database}`.`gp_clinical` WHERE read_2 IN ({formatted_codes})")
        
        # get earliest available date for duplicate eids
        if earliest_date:
            df = df.groupBy("eid").agg(F.min("event_dt").alias("event_dt"))
            
        return df


    def get_hesin_dates(self, df: DataFrame, earliest_date=True):

        hesin_df = self.spark.sql(f"SELECT dnx_hesin_id, epistart, epiend, admidate, disdate FROM `{self.database}`.`hesin`")

        df = df.join(hesin_df, on="dnx_hesin_id", how="inner")

        # get diagnosis date: takes the first non-null value since fields are in chronological order
        df = df.withColumn('diagnosis_date', F.coalesce('epistart', 'epiend', 'admidate', 'disdate'))

        # group by eid and get the earliest diagnosis year for each participant
        df = df.groupBy('eid').agg(F.min('diagnosis_date').alias('diagnosis_date'))

        return df.select("eid", "diagnosis_date")

    

    def get_death_dates(self, df: DataFrame):

        # get death dates
        dates_df = self.spark.sql(f"SELECT eid, date_of_death FROM `{self.ukb.database}`.`death`")

        # join on eid to filter out dates
        df = df.join(dates_df, on="eid", how="inner")

        # return death date
        df = df.select("eid", "date_of_death")

        return df


    def get_cancer(self, codes: list, unique_eids=True, earliest_date=True):
        distinct = "DISTINCT" if unique_eids else ""
        formatted_codes = ", ".join([f"'{code.replace('.', '')[:5]}'" for code in codes])

        # put fields into SQL query
        case_statements = [f"WHEN p40006_i{i} IN ({formatted_codes}) THEN p40005_i{i}" for i in range(22)]
        case_condition = " ".join(case_statements)

        query = f"""
        SELECT {distinct} eid, 
               CASE {case_condition} 
               ELSE NULL 
               END AS cancer_date
        FROM `{self.database}`.`participant_0073`
        WHERE { " OR ".join([f"p40006_i{i} IN ({formatted_codes})" for i in range(22)]) }
        """

        df = self.spark.sql(query)

        # get earliest available date for duplicate eids
        if earliest_date:
            df = df.groupBy("eid").agg(F.min("cancer_date").alias("cancer_date"))
            
        return df
        

 
    def get_rap_phenos(self, codes: list):
        with open('field_lookup.json', 'r') as json_file:
            lookup_dict = json.load(json_file)
        lookup = [(code, lookup_dict[code]) for code in codes]
        del lookup_dict
        
        if len(lookup) == 0:
            print("Error: Fields not found...")
            return
        
        for code, table in lookup:
            print(f"{code} found in {table}")
        
        code, table = lookup[0]
        df = self.spark.sql(f"SELECT eid, {code} FROM `{self.database}`.`{table}`")    

        for code, table in lookup[1:]:
            new_df = self.spark.sql(f"SELECT eid, {code} FROM `{self.database}`.`{table}`")
            df = df.join(new_df, on="eid", how="outer")

        return df




    def get_database_from_nexus(self):
        dataset_file = None
        try:
            # execute the `dx ls` command
            result = subprocess.run(
                ["dx", "ls", "/"],
                text=True,
                capture_output=True,
                check=True
            )

            files = result.stdout.splitlines()
            dataset_file = [file[:-8] for file in files if file.endswith('.dataset')][0]

        except subprocess.CalledProcessError as e:
            print(f"Error listing DNA nexus databases: {e.stderr}\nPlease define database manually using set_database(database_name)")
            self.find_databases()

        if dataset_file:
            print(f"Running on database {dataset_file}...")
            self.database = dataset_file
        else:
            print("No .dataset files found in nexus directory... Please define database manually using set_database(database_name)")
            self.find_databases()


    def find_databases(self):
        try:
            databases = [row.namespace for row in self.spark.sql("SHOW DATABASES").collect()]
            print(f"Found {len(databases)} available databases:")
            for i, db in enumerate(databases):
                print(f"{i + 1}. {db}")
            
        except Exception as e:
            print(f"Error finding available databases: {e}")

        
    def set_database(self, new_database: str):
        self.database = new_database

        
    def write_df_to_file(self, df: DataFrame, filename: str):
        pdf = df.toPandas()
        pdf.to_csv(filename, index=False, encoding="utf-8")
        print(f"{filename} saved successfully")

        
    def __str__(self):
        return f"PySpark running on database '{self.database}'"

