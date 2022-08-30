# Grasshopper Data Engineering Case Study

## Naive implementation

### Brief Introduction

*Disclaimer*

This naive implementation just read input csv and produce output csv for demonstration purpose

For full approach by designing a data warehouse, please refer to section 3.

### Implement Characteristic

Choice of Tech: Jupyter Notebook / Python

Prerequisite: Spark 3.3.0


## How to use the script

### Method 1 (Recommended): Kaggle / Jupyter Notebook
Interactive Kaggle Demonstration: https://www.kaggle.com/vuduclong0309/grasshopper-datatask

You can just fork the notebook directly and run. The output csv data would be downloadable under working/kaggle/output/l3data folder

You can also find the file in src/grasshopper-datatask.ipynb folder and import it to any jupyter notebook.

### Method 2: Offline script
Prerequisite: Spark 3.3
```
pip install pyspark
```

You can use the offline_script.py to run by doing following steps:
1. Create folder '/kaggle/input', put input file in that folder
2. Modify input file in the offline script to match your input file
3. Run the script e.g
`
  python offline_script.py
`
4. Collect output csv at output/l3_data folder
