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

# The Stuff You Can't See

Clearly, this code relies heavily on MongoDB and Airflow. That means most of the fruits of my labor are not really apparent in the scripts alone. Below are some images that show what is being prodced from this code:

Data pulled from Library of Congress API and pushed to a fresh MongoDB Atlas database:

#### Data loaded into MongoDB
![Alt text](/documentation/MongoDB_Loaded.png?raw=true "Data loaded into MongoDB")
#### Index data example
![Alt text](/documentation/MongoIndex_Loaded.png?raw=true "Index data example")
#### Details data example
![Alt text](/documentation/MongoDetail_Loaded.png?raw=true "Details data example")


Resulting plot from database query and data transformation using Pandas and MatPlotLib:
#### Output plot example
![Alt text](/documentation/newspaper_freq.png?raw=true "Output plot example")


# Improvement Areas

Of course, this is by no means an especially efficient process seeing as it pushes and pulls the same data on the same DAG flow with no purpose other than my own edification 

Aside from redundant educational processes, there are a number of things I would change if this code had to be put into actual use:
 - User credentials for MongoDB are passed as string environmental variables, rather than an access token
   - I didn't want to pay for MongoDB Atlas :)
 - The visualization is rudimentary and flat, it isn't going to be useful for much since it's just a placeholder (i.e. I got the data here and had to do something with it!)
 - There's limited Exception handling, a skill I need to work on but ran out of time to really pursue
 - I need more practice with git and multi-module files: I think there's a lot of unneeded information passing back and forth
 - I'm sure I'm not using parallel processing to its full potential
 - There's likely a lot of room to improve efficiency, I need an experienced dev to help point me in the right direction
