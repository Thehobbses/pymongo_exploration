# Project Impetus

Project to gain experience with several platforms and skills:
 - Airflow
 - MongoDB Atlas and PyMongo
 - Google Cloud Services
 - Parallelization
 - API to DB pipelines
 - Data visualizaton with Matplotlib

# Project Overview

I put together a pipeline that:
 - runs on a Google Compute Engine instance running Airflow
 - automatically calls the Library of Congress API
   - currently grabbing historical newspapers, but I built the code to be fairly easily pointed elsewhere
 - ingests that data and cleans it on parallel processes
 - posts refreshed data to a NoSQL server
 - queries that server and pulls the data out of it
 - transforms semi-structured data to flat Pandas dataframes
 - produces a line plot visualization with MatPlotLib

The pipeline is on a daily schedule, but that is easily adjusted in the DAG file

#### _Note_: this script will not run locally without updating the requirements.txt (because airflow is pre-installed in the Google Composer environment and it gets mad if you include it in your requirements). Once that is done, one would need to get their own MongoDB set up, load their credentials into the env. vars and edit the uri with the connection link Mongo provides

# The Stuff You Can't See

Clearly, this code relies heavily on MongoDB and Airflow. That means most of the fruits of my labor are not really apparent in the scripts alone. Below are some images that show what is being produced from this code

----------------------------------------------------------------------------------------------------------------------------------------

#### Google Cloud Service environments deployed:

![Alt text](/documentation/Airflow_DAG_prerun.png?raw=true "DAG Tree")

^ _Airflow found all its modules_



![Alt text](/documentation/Airflow_DAG_pipeline.png?raw=true "DAG Code deployed")

^ _DAG pipeline_



![Alt text](/documentation/GCS_Composer_Environment.png?raw=true "Google Composer environment")

^ _Composer environment that handles Airflow and dependencies_



![Alt text](/documentation/GCS_Bucket.png?raw=true "Data loaded into GCS bucket")

^ _Google Storage bucket_

----------------------------------------------------------------------------------------------------------------------------------------

#### Data pulled from Library of Congress API and pushed to a fresh MongoDB Atlas database:

![Alt text](/documentation/MongoDB_Loaded.png?raw=true "Data loaded into MongoDB")

^ _Data loaded into MongoDB_



![Alt text](/documentation/MongoIndex_Loaded.png?raw=true "Index data example")

^_Index data example_



![Alt text](/documentation/MongoDetail_Loaded.png?raw=true "Details data example")

^ _Details data example_

----------------------------------------------------------------------------------------------------------------------------------------

#### Resulting plot from database query and data transformation using Pandas and MatPlotLib:

![Alt text](/documentation/newspaper_freq.png?raw=true "Output plot example")

^ _Output plot example_

----------------------------------------------------------------------------------------------------------------------------------------

# Improvement Areas

Of course, this is by no means an especially efficient process seeing as it pushes and pulls the same data on the same DAG flow with no purpose other than my own edification 

Aside from redundant educational processes, there are a number of things I would change if this code had to be put into actual use:
 - User credentials for MongoDB are passed as string environment variables, rather than an access token
   - I didn't want to pay for MongoDB Atlas :)
 - The visualization is rudimentary and flat, it isn't going to be useful for much since it's just a placeholder (i.e. I got the data here and had to do something with it!)
 - There's limited exception handling, a skill I need to work on but ran out of time to really pursue
 - I need more practice with git and multi-module files: I think there's a lot of unneeded information passing back and forth
 - I'm sure I'm not using parallel processing to its full potential
 - There's likely a lot of room to improve efficiency, I need an experienced dev to help point me in the right direction
 - Got caught in dependency hell! Docker helped somewhat, but I ended up just adjusting my requirements.txt until it worked as intended
