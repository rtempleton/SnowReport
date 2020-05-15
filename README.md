# SnowReport
SnowReport is a Java app that helps pull together details about your Snowflake account into a concise report. The output of the reports is written back to the console in JSON form. You can cut and paste this output into any JSON editor/visualizer to browse the details of the report. I personally like to the the Chrome plug-in JSON Editor which can be downloaded here: https://chrome.google.com/webstore/detail/json-editor/lhkmoheomjbkfloacpgllgjcamhihfaj

There are currently two reports you can run in SnowReport  
**RoleHierarchy** - This report creates a tree of ROLES defined in account, lists the USERS and also details all Database and Schema privileges for the given role  
![RoleHierarchyScreenShot.PNG](https://github.com/rtempleton/SnowReport/blob/master/img/RoleHierarchyScreenShot.PNG)

**DatabaseSummary** - This report creates a tree of all DATABASES in the account and details all sequences, streams, procedures, functions, tasks, stages, and pipes within each SCHEMA  
![DatabaseSummaryScreenShot.PNG](https://github.com/rtempleton/SnowReport/blob/master/img/DatabaseSummaryScreenShot.PNG)  

## Running SnowReport

You need to provide a properties file with the following entries:  
`URI=jdbc:snowflake://<YourAccountURL>/?warehouse=DEMO&role=ACCOUNTADMIN`  
NOTE: Make sure you include the virtual warehouse to use and the ACCOUNTADMIN role in the connection string. An XS warehouse is fine  
`USER=<A user with AccountAdmin privileges>`  
`PASSWORD=<The users password>`  
`CONCURRENT_TASKS=<number of concurrent threads>`  
The database summary has to make several queries to the INFORMATION_SCHEMA in each database.  
`TIMEOUT_MINUTES=<number of minutes to wait>`  
In the event there’s a delayed response in our queries, this will prevent the app from hanging indefinitely  

In the terminal run: java -jar pathToSnowReport.jar pathToPropertiesFile  

![ExampleScreenShot.PNG](https://github.com/rtempleton/SnowReport/blob/master/img/ExampleScreenShot.PNG)


