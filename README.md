# scoring-pipeline Py App

Scoring Pipeline
================
Scoring Pipeline is a python app that can be used to perform ETL transformations followed by scoring on a deployed model, on a stream of records. The result is then either sent back to the client posting the request or queued up on the kafka sink topic, depending upon the mode that the app was configured in at the time of initialization.

The ETL transformations currently supported in Scoring Pipelines are:
 - add_columns
 - drop_columns
 - rename_columns
 - filter
 - score

Scoring pipeline can be initialized in 2 modes: Kafka streaming mode or REST endpoint streaming mode.

There are two ways to start Scoring Pipeline, locally and on TAP. This section covers how to run scoring pipeline locally and instantiate it in TAP.


Running Scoring Pipeline locally
--------------------------------

After cloning the repository, CD to CLONED_DIR/scoring-pipelines/scoring_pipelines.

Copy the tar archive (advscore.tar) containing the **configuration file** (config.json) and the **python script** (test_script.py)to be executed into this dir. 

Run the app:
 $ ipython scoringExecutor.py advscore.tar

The application is now running on default debug port: 5000 

In order to run the app on a different port (say 9100):
 $ ipython scoringExecutor.py advscore.tar 9100

The application is now running on port: 9100 

You can now post requests to the scoring pipeline using curl command as follows:

    curl -H "Content-type: application/json" -X POST -d '{"message": "4/3/2016 10:32, P0001,1,0.0001,....., 192,-4.1158,192,3.8264"}' http://localhost:9100/v2/score


Create a Scoring Pipeline Instance from a broker in TAP
-------------------------------------------------------

In the TAP web site:

1) Navigate to **Services -> Marketplace**.

2) Select **scoring_pipeline** => **Create new instance**.

3) Fill in an instance name of your choice *(given below as **etlScoring**.

4) You will be able to see your scoring pipeline under the Applications page and obtain its URL.

5) Now call its REST endpoint to load the tar archive (advscore.tar) containing the **configuration file** (config.json) and the **python script** (test_script.py)to be executed.
    curl -i -X POST -F file=@advscore.tar  "http://etlScoring.demotrustedanalytics.com"

6) The configuration file needs the following fields in there:

    "file_name" -- python script that needs to be executed on every streaming record <test_script.py>

    "func_name" -- name of the function in the python script that needs to be invoked <evaluate>

    "input_schema" -- schema of the incoming streaming record

    "src_topic" -- kafka topic from where to start consuming the records (in case of Kafka streaming) else this field should be empty <input>

    "sink_topic" -- kafka topic to which the app starts writing the predictions (in case of Kafka streaming) else this field should be empty <output>

    Note: For Kafka Streaming, both source and sink topics need to be specified

Scoring Pipeline Config file template
-------------------------------------

Below is a sample json to configure scoring pipeline in kafka streaming mode:

.. code::

    { "file_name":"test_script.py", "func_name":"evaluate", "input_schema": {
            "columns": [
              {
                "name":"field_0",
                "type":"str"
              },
              {
                "name":"field_1",
                "type":"str"
              },
              .....

              {
                "name":"field_162",
                "type":"str"
              }
            ]}, "src_topic":"input", "sink_topic":"output"}

Below is a sample json to configure scoring pipeline in REST endpoint streaming mode:

.. code::

    { "file_name":"test_script.py", "func_name":"evaluate", "input_schema": {
            "columns": [
              {
                "name":"field_0",
                "type":"str"
              },
              {
                "name":"field_1",
                "type":"str"
              },
              .....

              {
                "name":"field_162",
                "type":"str"
              }
            ]}, "src_topic":"", "sink_topic":""}



Scoring Pipeline Python Script Example
--------------------------------------

.. code ::

    from record import Record
    import atktypes as atk
    import numpy as np


    def add_numeric_time(row):
        try:
            x = row['field_0'].split(" ")
            date = x[0]
            time = x[1]
            t = time.split(":")
            numeric_time = float(t[0]) + float(t[1])/60.0+float(t[2])/3600.0
            return [date, numeric_time]
    	except:
            return ["None", None]

    def string_to_numeric(column_list):
        def string_to_numeric2(row):
            result = []
            for column in column_list:
                try:
                    result.append(float(row[column]))
                except:
                    result.append(None)
            return result
        return string_to_numeric2

    def drop_null(column_list):
        def drop_null2(row):
            result = False
            for col in column_list:
                result = True if row[col] == None else result
            return result
        return drop_null2

    column_list = ['field_'+ str(x) for x in range(19,136) if np.mod(x,4)==3]
    new_columns_schema = [('num_' + x, atk.float64) for x in column_list]

    PCA_column_list = ['num_field_'+ str(x) for x in range(19,136) if np.mod(x,4)==3]

    y= ['field_'+str(i) for i in range(0, 163)]
    y.extend(['Venus'])
    y.extend(['Mercury'])

    # the entry point method that will be invoked by the scoring pipeline app on each streaming record
    def evaluate(record):
        record.add_columns(add_numeric_time, [('date', str), ('numeric_time', atk.float64)])
        record.add_columns(string_to_numeric(column_list), new_columns_schema)
        record.rename_columns({'date':'Venus', 'numeric_time':'Mercury'})
        record.drop_columns(y)
        result = record.filter(drop_null(PCA_column_list))
        print("result is %s" %result)
        if not result:
	    # send the record to be scored on the model that is already running on TAP
    	    r = record.score("scoringengine2.demotrustedanalytics.com")
    	    return r


7) If the scoring pipeline was configured to work with Kafka messaging queues then start streaming records to the source-topic.

8) If the scoring pipeline was configured to use the REST endpoints, then you can post requests using curl command as follows:
    curl -H "Content-type: application/json" -X POST -d '{"message": "4/3/2016 10:32, P0001,1,0.0001,....., 192,-4.1158,192,3.8264"}' http://etlscoring.demotrustedanalytics.com/v2/score






import os
import csv
import json
import numpy as np
import socket,struct
import time as time
import itertools


import trustedanalytics as atk

print "ATK installation path = %s" % (atk.__path__)

atk.server.uri = 'atk-8706.demo-gotapaas.com'
atk.connect(r'/atk-8706.creds')

hostname = os.environ['HOSTNAME']

dataset = "hdfs://nameservice1/org/intel/hdfsbroker/userspace/689ffe9c-032d-4913-b6b7-d9af74f00ce6/47f53557-7b4e-4d1d-929b-7f4e634bf173/000000_1"
schema=[("field_"+str(x), str) for x in range(163)]

data = atk.Frame(atk.CsvFile(dataset, schema,  skip_header_lines=1))

def add_numeric_time(row):
    try:
        x = row['field_0'].split(" ")
        date = x[0]
        time = x[1]
        t = time.split(":")
        numeric_time = float(t[0]) + float(t[1])/60.0+float(t[2])/3600.0
        return [date, numeric_time]
    except:
        return ["None", None]

data.add_columns(add_numeric_time, [('date', str), ('numeric_time', atk.float64)])

data.inspect(wrap=10, width=100)

def string_to_numeric(column_list):
    def string_to_numeric2(row):
        result = []
        for column in column_list:
            try:
                result.append(float(row[column]))
            except:
                result.append(None)
        return result
    return string_to_numeric2

column_list = ['field_'+ str(x) for x in range(2,163)]
new_columns_schema = [('num_' + x, atk.float64) for x in column_list]

data.add_columns(string_to_numeric(column_list), new_columns_schema)
data.inspect(wrap=10, width=100)

def drop_null(row):
    result = False
    for key, value in row:
        result = True if value == None else result
    return result

no_gt136 = data.copy()
x= ['field_'+str(i) for i in range(137, 163)]
x.extend(['num_field_'+str(j) for j in range(137, 163)])
no_gt136.drop_columns(x)
no_gt136
print no_gt136.inspect(wrap=10,width=100)
print no_gt136.row_count

no_gt136.drop_rows(drop_null)
print no_gt136.row_count
print no_gt136.inspect(wrap=10,width=100)

num_nulls = 53820 - 49234
print num_nulls


if drop_objects == True:
    drop('pcaModel_' + reference)

PCA_model = atk.PrincipalComponentsModel('pcaModel_' + reference)
PCA_model.train(no_gt136,  
                ["num_field_"+str(x) for x in range(3,137) if np.mod(x,2)==1],
                mean_centered=True, k = 20)

scored_frame = PCA_model.predict(no_gt136, mean_centered=True, t_squared_index=True, 
                                 observation_columns=["num_field_"+str(x) for x in range(3,137) if np.mod(x,2)==1], 
                                 name ='scored_frame')

scored_frame.inspect(wrap=10, width=100)

cols = ['t_squared_index', 'field_1', 'numeric_time','date']


df = pd.DataFrame(scored_frame.take(15000, columns=cols), 
                  columns=cols)



