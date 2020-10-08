# SampleETL
Sample simple etl moving data in large local CSV file to Mongodb via Spark SQL

## To reproduce work

### Install Spark
Download Spark 2.4.4 from https://archive.apache.org/dist/spark/spark-2.4.4/
Make sure it's built using Scala 2.11. You can check this by running `./spark-shell master="local[2]"`
from the bin directory of your Spark installation, and checking the log output on startup for the
Scala version. You can change the Scala version by following the instructions at https://spark.apache.org/docs/2.1.1/building-spark.html.

### Install MongoDB
Install mongodb-community 4.4 using the instructions for your OS, e.g. https://docs.mongodb.com/manual/tutorial/install-mongodb-on-os-x/.
Run the following sequence of commands to connect to the instance and set up your database:
1. `mongo`
2. `use ais`
3. `db.createCollection('ais_data')`
4. `db.createCollection('duplicates_count')`
5. `db.createCollection('most_common_value')`
6. `db.createCollection('nulls_count')`
7. `db.createCollection('total_rows')`
8. `db.createCollection('unique_values')`

### Install SBT
Since this is a Scala project you will need the Scala Build Tool to build the fat jar that we'll submit to Spark.
I use the SBT plugin for IntelliJ, but you can follow the instructions at https://www.scala-sbt.org/1.x/docs/Setup.html 
for your chosen environment. I used version 1.3.13.

### Install Python Dependencies
I used anaconda3 to manage dependencies, but use the Python dependency management tool of your choice. I installed `Python 3.7.4`, along with 
`pandas` and `pymongo`.

### Running App
Navigate to the root of the Project and run `sbt 'set test in assembly := {}' clean assembly` to build the jar. Then
navigate to the `bin` directory of your Spark installation and run the following command 
`./spark-submit --class "com.sample.etl.ETL" --master local[4] /Users/colmginty/Projects/SampleETL/target/scala-2.11/SampleETL-assembly-0.1.jar`.
This should launch the Spark job locally, which should take about 15 minutes to run (you should see lots of log output in the terminal).
Run `db.ais_data.find()` in the mongo shell and you should see the data in there. You could repeat that command for each of the other (summary data)
tables.

### Running Report
Navigate to the root of the project and run `python ./ais_report.py`.

## Sample Report Output
You should see something like the following (this output shows summary for data in one row)

#### total_rows
0    10219558

### duplicates
Timestamp  count

0      19/01/2019 00:06:42    113
1      19/01/2019 00:11:36    102
2      19/01/2019 00:12:40    131
3      19/01/2019 00:13:42    133
4      19/01/2019 00:14:39    110
...                    ...    ...
86394  19/01/2019 23:37:19     98
86395  19/01/2019 23:50:09    112
86396  19/01/2019 23:50:34    122
86397  19/01/2019 23:51:10    127
86398  19/01/2019 23:53:47     97

#### max value    
max(count)

0         214
1         214

#### nulls
Timestamp

0            0

#### unique values

Timestamp  Type of mobile  MMSI  Latitude  ...    A    B   C   D
0        91668               6  3091   1806967  ...  214  178  44  41

## Summary of Approach
I've manually copied a CSV file from the AIS ftp server to my local machine, read that into Spark, computed summary data,
and written both the AIS data and summary data to MongoDB. I have a standalone Python script for viewing the summary data
at the command line. Obviously there are much better ways to do most of these things given more time, which we will 
hopefully discuss in the call on Friday.


  


