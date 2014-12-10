# DynamoDB CSV Loader

Since I had a task of finishing the initial import of 120 GB of data to Dynamo DB in a couple of days I did not have patience 
to keep fighting and struggling with the AWS Data Pipeline. I've tried it for three times and didn't manage to make it work.
When googling about it I didn't find much help.

To solve this I've quickly got together some code and built a small java application that uses threads to push data to Dynamo DB having csv files as
datasource. Feel free to improve this tool and make the code better :)

# Usage

 java -jar dynamo-db-loader.jar --table User --threads 50 --header Name,Email,Phone,Age,Birthday,Country,Language users_table_dump.csv

The data will be persisted as key-pairs in Dynamo DB:

{ "Name" : "Matthew Oak", "Email" : "matto@gmail.com", "Phone" : "+31232133213" }	 

# Build

mvn package

# AWS Credentials config

You must have the ~/.aws/config file setup in your environment. If you have installed the aws cli you're good to go.