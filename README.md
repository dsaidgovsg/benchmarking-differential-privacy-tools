# Benchmarking Differential Privacy Python Tools 
------------------------

This repository is the implementation of the benchmarked differential privacy python libraries and frameworks. 

![Benchamarked tools](images/benchmarked_tools.png)

The libraries and frameworks are evaluated on utility and execution time. 

## Sample Experimental Results 
-----------------------

- Mean relative error of the sum query for experiments with synthetic datasets.  


![Utility Analysis](images/sum_utility_analysis.png)

- Execution time of the tools – supporting the distributed environment, experimented on synthetic datasets of varying sizes.


![Execution Time](images/spark_execution_time.png)


## Executing Queries using the Libraries/Frameworks on Synthetic Data 
------------------------

```bash
# sample command to run a query on a dataset size using a library/framwework 
python3 run_tool.py --size 100k --query VARIANCE  --tool opendp
```

 Argument | Description | Type | Default 
| ---- | --- | --- | --- |
| size | Dataset size to run query. Valid values are {`1k`, `10k`, `100k`}| str | `10k` |
| query | Query to run. Valid values are { `count`, `sum`, `mean`, `variance`} | str | `count` |
| tool | Library/Framework to use. Valid values are { `diffprivlib`, `opendp`, `tmlt_ana`, `pipelinedp_local`, `pipelinedp_spark`} | str | `diffprivlib` |
