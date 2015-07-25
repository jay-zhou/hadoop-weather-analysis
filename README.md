# hadoop-weather-analysis
The project is to download weather history data for most of the countries in the world and put data to HDFS. After data is put in HDFS, mapper and reducer jobs run against it and saved the analysis results to HBase. The code is developed and executed on Hadoop 2.8 using Java and Hbase as the NoSQL database.
#### Here are steps to run through the application
######1. Run the shell scripting and python code to parse the webpage to get all country codes, and use country code to download xml files for all countries
All the XML files are saved as xml_files/weather_xxx.xml (xxx is the country code)
######2. Copy the xml files to HDFS

```
hadoop fs -mkdir /user
hadoop fs -mkdir /user/hadoop
hadoop fs -mkdir /user/hadoop/data
hadoop fs -ls /user/hadoop/data
hadoop fs -copyFromLocal /home/hadoop-weather-analysis/xml_files /user/hadoop/data/
```

######3. Create weather tables in HBase database
```
create 'weather', 'mp'
create 'weather_sum', 'mp'
```
#####4. Load xml files from HDFS to weather table in HBase
`hadoop jar loadXml2.jar com.jzhou.hbase.dataload.HBaseDriver /user/hadoop/data/xml_files  /out1 weather`

#####5. Check the data in the HBase table 
```
count 'weather'
t = get_table 'weather'
t.scan
```

#####6. Process data to get the monthly data for past 10 years and save back to HBase table

`hadoop jar processweather.jar com.jzhou.hbase.process.DBDriver` 

#####7. Check the results in the HBase table

`scan 'weather_sum'`


