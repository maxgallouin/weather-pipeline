## (Senior) Data Engineer - Enpal Energy (f/m/x) - Take-Home Coding Test

## Task Overview and Context
At Enpal Energy we compute forecasts for the Energy Market via Machine Learning
algorithms. A major input to these forecasts are weather data. The DWD 
("Deutscher Wetterdienst"; engl.: "German Meteorological Service") provides
weather forecasts and observations as open data. An open source project, called
Brightsky (see section [Sources](#sources)), provides a convenient wrapper to 
serve this data via a REST-API. Your task is to write a data ingestion and 
transformation pipeline for the weather data that can serve a downstream 
ML-pipeline.


## Task description
1. Discover and understand the provided data sources, see Section 
   [Sources](#sources).
2. Set up a database/data storage of your choice for storing the data to ingest 
   and design appropriate data models.
3. Create a python ingestion script for the geo shapes of german postal codes, 
   see section [Sources](#sources) and ingest the data to your storage.
4. Create two ingestion pipelines for (a) weather observations per station and 
   (b) weather forecasts per station from BrightSky-API using the python 
   programming language and additional tools of your choice. You can limit the 
   ingestion to a reasonable spatial scope of your choice, e.g. a 3-digit postal 
   code area or a German federal state. We aim to continuously ingest the 
   freshest data possible. 
5. Create a data transformation pipeline on top of your ingested data that 
   serves data models of **cleaned weather forecasts and observations per postal
   code in 1h temporal resolution**. Use any tools of your choice. Please 
   realize at least 2 reasonable data-cleaning/data-validation steps, and 
   outline additional steps, that come to your mind. Keep in mind, that the 
   resulting data models should be ready for downstream usage in ML pipelines.
6. Use a task scheduling framework of your choice to run all 
   ingestion-/transformation pipelines on schedule.
7. Think about how to productionize your pipeline and be prepared to
   discuss this in the interview.
8. We estimate that the completion time for this task is 7-8 hours. Please don't
   spend more time, see Section [Additional Remarks](#additional-remarks).


## Required Skillset
* discover and understand new data sources
* automate reliable data ingestions
* provide useful data transformations
* work with geospatial and timeseries data
* create comprehensive data models
* manage a tech stack with:
  * python and SQL programming language,
  * task orchestration, 
  * and database management


## Submission
* Please submit your work via a private git-repository link or a compressed 
  archive. 
* Make sure to include a comprehensive documentation. 
* Your application should be portable and executable: 
  * Therefor please use appropriate frameworks of your choice, 
    e.g. `virtualenv`, `conda`, `poetry`, `pipenv`, `uv`, `docker`, 
    `docker-compose`, `kubernetes`.
  * add documentation on how to run your app
* Please don't submit any data or database dumps. The data should only be 
  handled by code.
* Please justify choices on the tools/tech stack that you apply.


## Additional Remarks
* The requirements for this task are not specified in great detail. We encourage
  you to come up with your own thoughts and justified decisions whenever you 
  encounter ambiguity. How you deal with this ambiguity is a dedicated part of 
  the assessment of your work.
* We acknowledge that you work on this coding challenge in your free time. If
  you feel that the scope is too large, feel free to chose simple (but working)
  shortcuts and provide code stubs or comments on how to realize a more 
  elaborate/production-ready setup. That will not be interpreted negatively for
  you, if you can explain them in the interview.
* You are explicitly invited to use AI coding agents to help you with this task. 
  Please make sure that you understand and can assess every piece of code that 
  you submit. 


## Sources
* BrightSky API: https://brightsky.dev/
* German Zip Code Shapes: https://github.com/yetzt/postleitzahlen
