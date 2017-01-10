# scoring-pipeline Py Application

The Scoring Pipeline is a Python application that can be used to perform ETL transformations followed by scoring on a deployed model (via the Scoring Engine), on a stream of records. The result is then either sent back to the client posting the request or queued up on the Kafka sink topic, depending upon the mode that the application was configured during initialization.

The ETL transformations currently supported in Scoring Pipelines are:  

 - add_columns
 - drop_columns
 - rename_columns
 - filter
 - score

The Scoring Pipeline can be initialized in 2 modes: Kafka streaming mode or REST endpoint streaming mode.

There are two ways to start Scoring Pipeline, locally and on TAP. This section covers how to run the Scoring Pipeline locally and instantiate it in TAP.

>If you don't need to perform transformations prior to scoring, you can use the [Scoring Engine](https://github.com/trustedanalytics/scoring-engine) instead of the Scoring Pipeline.  

##Running Scoring Pipeline locally

After cloning the repository, `cd` to `CLONED_DIR/scoring-pipelines/scoring_pipelines`.

Copy the tar archive (`advscore.tar`) containing the **configuration file** (`config.json`) and the **python script** (`test_script.py`) to be executed into this dir. 

Run the app:  
    `$ ipython scoringExecutor.py advscore.tar`

The application is now running on default debug port: 5000 

In order to run the app on a different port (say 9100):
    `$ ipython scoringExecutor.py advscore.tar 9100`

The application is now running on port: 9100 

You can now post requests to the scoring pipeline using curl commands as follows:

    curl -H "Content-type: application/json" -X POST -d '{"message": "4/3/2016 10:32, P0001,1,0.0001,....., 192,-4.1158,192,3.8264"}' http://localhost:9100/v1/score 

##Create a Scoring Pipeline instance from a broker in TAP

From the TAP Console:

1) Navigate to **Services > Marketplace**.

2) Search for (or scroll to) **TAP Scoring Pipeline** and select it.

3) Fill in an instance name of your choice (shown below as `etlScoring`) and click the **Create new instance** button.

>This may take a minute or two to complete.

4) When done, you can see your scoring pipeline in the **Applications** page and obtain its URL.

5) Now call the scoring pipeline's REST endpoint to load the tar archive (`advscore.tar`) containing the **configuration file** (`config.json`) and the **python script** (`test_script.py`) to be executed.
    curl -i -X POST -F file=@advscore.tar  "http://etlScoring.demotrustedanalytics.com"

6) The configuration file needs the following fields included:

    "file_name" -- python script that needs to be executed on every streaming record **test_script.py**

    "func_name" -- name of the function in the python script that needs to be invoked **evaluate**

    "input_schema" -- schema of the incoming streaming record

    "src_topic" -- kafka topic from where to start consuming the records (in case of Kafka streaming) else this field should be empty **input**

    "sink_topic" -- kafka topic to which the app starts writing the predictions (in case of Kafka streaming) else this field should be empty **output**

    Note: For Kafka Streaming, both source and sink topics need to be specified

##Scoring Pipeline config file template

The JSON sample below configures the scoring pipeline for Kafka streaming mode:

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

The JSON sample below configures the scoring pipeline for REST endpoint streaming mode:

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



##Scoring Pipeline Python Script Example

.. code ::

    from record import Record
    import atktypes as atk
    import numpy as np

    # the following 3 lambdas/UDFs were taken as is from the batch processing script that was used to train in the model during development phase.

    # take the first column from the input row and split it into date and time 
    def **add_numeric_time(row)**:
        try:
            x = row['field_0'].split(" ")
            date = x[0]
            time = x[1]
            t = time.split(":")
            numeric_time = float(t[0]) + float(t[1])/60.0+float(t[2])/3600.0
            return [date, numeric_time]
    	except:
            return ["None", None]

    # takes a list of columns and for every input row those column values(strings) are converted to floats and a list of floats is returned
    def **string_to_numeric(column_list)**:
        def string_to_numeric2(row):
            result = []
            for column in column_list:
                try:
                    result.append(float(row[column]))
                except:
                    result.append(None)
            return result
        return string_to_numeric2

    # takes a list of columns and for every input row those column values are evaluated. True is returned if any of those column values are None; false otherwise
    def **drop_null(column_list)**:
        def drop_null2(row):
            result = False
            for col in column_list:
                result = True if row[col] == None else result
            return result
        return drop_null2

    # preparing a column list for passing to 'string_to_numeric' UDF
    column_list = ['field_'+ str(x) for x in range(19,136) if np.mod(x,4)==3]
    # preparing a schema to go with the new columns that would be added  
    new_columns_schema = [('num_' + x, atk.float64) for x in column_list]    

    # preparing a list of columns that would be dropped from every input row
    y= ['field_'+str(i) for i in range(0, 163)]
    y.extend(['Venus'])
    y.extend(['Mercury'])

    # Selecting final column list that would be checked for any null values
    PCA_column_list = ['num_field_'+ str(x) for x in range(19,136) if np.mod(x,4)==3]

    # the entry point method that will be invoked by the scoring pipeline app on each streaming record. This would need to be referenced in the configuration file as shown above
    def evaluate(record):
        record.add_columns(add_numeric_time, [('date', str), ('numeric_time', atk.float64)])
        record.add_columns(string_to_numeric(column_list), new_columns_schema)
        record.rename_columns({'date':'Venus', 'numeric_time':'Mercury'})
        record.drop_columns(y)
        result = record.filter(drop_null(PCA_column_list))
        print("result is %s" %result)
        if not result:
	    # send the record to be scored, to the model that is already running in Scoring Engine on TAP
    	    r = record.score("scoringengine2.demotrustedanalytics.com")
    	    return r

>For more information on the Scoring Engine, visit: http://trustedanalytics.github.io/atk/versions/master/ad_scoring_engine.html

7) If the Scoring Pipeline was configured to work with Kafka messaging queues, then start streaming records to the source-topic.

8) If the Scoring Pipeline was configured to use the REST endpoints, then post requests using a curl command as follows:
    curl -H "Content-type: application/json" -X POST -d '{"message": "4/3/2016 10:32, P0001,1,0.0001,....., 192,-4.1158,192,3.8264"}' http://etlscoring.demotrustedanalytics.com/v2/score


Note that the script used to do transformations on streaming records in the Scoring Pipelines application can very easily be derived from the script used in model training using batch processing. The batch script below was used to generate the test_script used in the previous example.

.. code::    
    
    ...
    . set up code
    ..

    import trustedanalytics as atk

    atk.server.uri = 'atk-8706.demo-gotapaas.com'
    atk.connect(r'/atk-8706.creds')
    dataset = "hdfs://nameservice1/org/intel/hdfsbroker/userspace/689ffe9c-032d-4913-b6b7-d9af74f00ce6/47f53557-7b4e-4d1d-929b-7f4e634bf173/000000_1"
    schema=[("field_"+str(x), str) for x in range(163)]

    data = atk.Frame(atk.CsvFile(dataset, schema,  skip_header_lines=1))

    def **add_numeric_time(row)**:
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

    ...
    . code to inspect the data
    ..

    def **string_to_numeric(column_list)**:
        def string_to_numeric2(row):
            result = []
            for column in column_list:
                try:
                    result.append(float(row[column]))
                except:
                    result.append(None)
            return result
        return string_to_numeric2   

    column_list = ['field_'+ str(x) for x in range(19,136) if np.mod(x,4)==3]
    new_columns_schema = [('num_' + x, atk.float64) for x in column_list]

    data.add_columns(string_to_numeric(column_list), new_columns_schema)
    
    ...
    . code to inspect the data
    ..


    def **drop_null(column_list)**:
        def drop_null2(row):
            result = False
            for col in column_list:
                result = True if row[col] == None else result
            return result
        return drop_null2

    
    PCA_column_list = ['num_field_'+ str(x) for x in range(19,136) if np.mod(x,4)==3]
    data_no_null = data.copy()
    data_no_null.drop_rows(drop_null(PCA_column_list))

    ...
    . code to inspect and do other counting and displaying operations on the data
    ..


    # train the model
    PCA_output= PCA_model.train(mariposa_no_null,  
                PCA_column_list,
                mean_centered=True, k = 30)

   

    ...
    . code for running the data through the model for verification
    . Download the data to Pandas for plotting
    . Create scatter plot for viewing
    . Examine Eigen Values
    . Examine Score for normality
    . Publish the model
    ..

