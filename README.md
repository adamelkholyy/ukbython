# ukpython: A tooling package for the UK Biobank RAP     
Work In Progress - ```ukbython``` allows simple querying, filtering, and data retrieval from the UK Biobank RAP with a variety of tools available, written for speed and ease of use.    
Setup: 
```python
import subprocess
subprocess.run(f"curl -L -o ukbython.py https://raw.githubusercontent.com/adamelkholyy/ukbython/main/ukbython.py", shell=True, check=True)
subprocess.run(f"curl -L -o field_lookup.json https://raw.githubusercontent.com/adamelkholyy/ukbython/main/field_lookup.json", shell=True, check=True)
```
