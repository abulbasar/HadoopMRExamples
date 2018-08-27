# HadoopMRExamples
Example of Hadoop Map Reduce program to find in year 2016 what is average volume per stock. Following is the SQL equivalent if the data stored in a database table. 
```
Select symbol, avg(volume) from stocks where year(date) = 2016 group by symbol 
```
More more instructions on how to build a maven project for Hadoop Map Reduce, look at the following article.
https://blog.einext.com/hadoop/creating-a-hadoop-mr-project-using-maven


To run this example
```
$ mkdir ~/workspaces
$ cd ~/workspaces
$ git clone https://github.com/abulbasar/HadoopMRExamples.git
$ cd HadoopMRExamples
```
Compile project into a jar file
```
$ mvn clean package
```

Download test data and upload to HDFS
```
$ wget https://raw.githubusercontent.com/abulbasar/data/master/stocks.small.csv
$ hadoop fs -mkdir stocks
$ hadoop fs -put stocks.small.csv stocks/
```
Run hadoop jar command to execute
```
$ hadoop jar target/HadoopMRExamples-1.0-SNAPSHOT.jar com.example.StockPriceCalculator stocks stocks_avg
```
Once the job is finished, check the output in to HDFS
```
$ hadoop fs -cat stocks_avg/*
```
