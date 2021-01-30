# trendyol-bootcamp-spark
## HOMEWORK DEFINITION
Find the latest version of each product in every run, and save it as snapshot.  <br/>

Product data stored under the data/homework folder. <br/>
Read data/homework/initial_data.json for the first run. <br/>
Read data/homework/cdc_data.json for the nex runs. <br/>

Save results as json, parquet or etc.

Note: You can use SQL, dataframe or dataset APIs, but type safe implementation is recommended.

## HOMEWORK SOLUTION
### ASSUMPTION
I assume that this job will run once in a day. In this assumption there is only one json file in each partition_date folder. <br/>
If it needs to be run more than one in a day, we need to get latest json in the partition_date folder. <br/>
### SOLUTION APPROACH
First, look at the batch output folder. If there is any exist data, read json as dataset and use it for merging with new dataset. <br/>
If there is no exist data, get initial data and use it for merging with new dataset. <br/>

### OUTPUT FOR INITIAL RUN
<img src="https://github.com/ahmetlekesiz/trendyol-bootcamp-spark/blob/master/homework_output/initial_output.PNG?raw=true" />

### OUTPUT AFTER INITIAL RUN
<img src="https://github.com/ahmetlekesiz/trendyol-bootcamp-spark/blob/master/homework_output/output_after_initial_run.PNG?raw=true" />
