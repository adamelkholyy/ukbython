# ukbython: Phenotype Derivation Tools for the UK Biobank RAP     
<strong>Adam El Kholy | [A.El-Kholy@exeter.ac.uk](mailto:A.El-Kholy@exeter.ac.uk) | Last updated: 13/02/2025       


</strong>   

*All data shown in this README is synthetic*

## [Read the docs here!](https://adamelkholyy.github.io/ukbython/ukbython.html) 

```ukbython``` allows for easy phenotype derivation in Python with simple querying, filtering, and data retrieval from the UK Biobank RAP. Features eid and earliest diagnosis date retrieval across all major coding schemas along with fast lookup and retrieval of any field in the database and functionality for custom queries using SQL with PySpark.     

Valid coding schemas
- ICD9
- ICD10
- Read2
- Read3
- OPCS3
- OPCS4            

Tables used for code lookup
- Hospital inpatient data
- GP clinical records
- Death records
- Cancer registry

## Installation 
<strong>To run `ukbython` start an instance of JupyterLab with SparkCluster on the UK Biobank RAP</strong>

To import `ukbython` using Python:
```python
import subprocess
subprocess.run(f"curl -L -o ukbython.py https://raw.githubusercontent.com/adamelkholyy/ukbython/main/ukbython.py", shell=True, check=True)
subprocess.run(f"curl -L -o field_lookup.json https://raw.githubusercontent.com/adamelkholyy/ukbython/main/field_lookup.json", shell=True, check=True)
```

To import `ukbython` using Git:
```bash
gh download adamelkholyy/ukbython -p "ukbython.py"  
gh download adamelkholyy/ukbython -p "field_lookup.json"  
```

Initialising `ukbython` and connecting to the UK Biobank database:
```python
from ukbython import ukbython
ukb = ukbython()
```
```
Running on database app103356_20241205153134...
```
## Examples

1. Retrieving eids with matching ICD10 codes and earliest diagnosis dates for a basic Heavy Menstrual Bleeding (HMB) phenotype:
```python
import subprocess
icd10s = [
    "N92.0",  # excessive and frequent menstruation with regular cycle
    "N92.1",  # excessive and frequent menstruation with irregular cycle
    "N92.2",  # excessive menstruation at puberty
    "N92.4"   # excessive bleeding in the premenopausal period
]
hmb_phenotype = ukb.get_icd10(icd10s)
```
```plaintext
Searching 4 ICD10 codes...
Found 14715 matching hospital records
Found 0 matching death records
Found 0 matching cancer records
Operation complete: Found 14715 matching records in 32.05 seconds.
```
```python
hmb_phenotype.show()
```
```
+------+----------------+
|  eid | diagnosis_date |
+------+----------------+
| 0001 |     1999-05-12 |
| 0002 |     1987-08-25 |
| 0003 |     2003-11-03 |
| 0004 |     1990-02-15 |
| 0005 |     2001-01-30 |
+------+----------------+
output truncated to first 5 lines
```

2. Looking up specific fields on the RAP:
```python
fields = ["p20004_i0_a0",  # self-reported operation (instance 0, array 0)
          "p20010_i0_a0"]  # date of operation (instance 0, array 0)
selfreport_operations_df = ukb.get_rap_field(fields)
```
```
Searching 2 RAP fields...
p20004_i0_a0 found in participant_0032
p20010_i0_a0 found in participant_0033
Operation complete: Found 502131 matching records in 14.26 seconds.
```
```python
selfreport_operations_df.show()
```
```
+------+--------------+--------------+
|  eid | p20004_i0_a0 | p20010_i0_a0 |
+------+--------------+--------------+
| 0001 |         1360 |   1998-02-22 |
| 0002 |            0 |   2002-06-15 |
| 0003 |         2456 |   2009-12-23 |
| 0004 |          789 |   1985-02-14 |
| 0005 |         1023 |   1991-05-01 |
+------+--------------+--------------+
output truncated to first 5 lines
```

3. Making your own queries to the UK Biobank database using SQL with PySpark
```python
# query for selecting female participants only (p31 is the sex field, 0 is the code for females)
query = f"SELECT DISTINCT eid FROM `{ukb.database}`.`participant_0001` WHERE p31 == 0"
females_df = ukb.spark.sql(query)
females_df.count()
```
```
273157
```

For any issues or queries please [read the docs](https://adamelkholyy.github.io/ukbython/ukbython.html) or contact [A.El-Kholy@exeter.ac.uk](mailto:A.El-Kholy@exeter.ac.uk)
