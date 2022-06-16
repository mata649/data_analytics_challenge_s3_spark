#  Data Analytics Challenge S3-RDS-SPARK

  

This ETL is inspired by the [Alkemy Data Analytics Challenge](https://cdn.discordapp.com/attachments/670996715083399199/942821808619520091/Challenge_Data_Analytics_con_Python.pdf), but was changed to implements **AWS S3, AWS RDS, and Spark**, I know that in some way do this with Spark is like killing an ant with a gun, but I think that it is a good challenge to practice.
This ETL process consists in get information from three different sources:

 - [Datos Argentina - Museos](https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_4207def0-2ff7-41d5-9095-d42ae8207a5d)
 - [Datos Argentina - Salas de Cine](https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_392ce1a8-ef11-4776-b280-6f1c7fae16ae)
 - [Datos Argentina - Bibliotecas Populares](https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_392ce1a8-ef11-4776-b280-6f1c7fae16ae)
 
 Once the data is downloaded we are going to organize the information in an S3 Bucket with the next format **{source}/{year-month}/{source}-{exact date}**, then the information is loaded from S3 to Spark's DataFrames to do the transformation and get the insights, and finally loaded to a database in AWS RDS.
 
 

##  Requirements

-  **AWS Services:** [AWS (Amazon Web Services)](https://aws.amazon.com/what-is-aws/) is the world’s most comprehensive and broadly adopted cloud platform, offering over 200 fully featured services from data centers globally. Millions of customers—including the fastest-growing startups, largest enterprises, and leading government agencies—are using AWS to lower costs, become more agile, and innovate faster.
- **Apache Spark 3.2.1:** [Apache Spark](https://spark.apache.org/docs/latest/) is a unified analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala, Python, and R, and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing.

-  **venv:** The [`venv`](https://docs.python.org/3/library/venv.html#module-venv  "venv: Creation of virtual environments.") module provides support for creating lightweight “virtual environments” with their own site directories, optionally isolated from system site directories.
- **Docker:** [Docker](https://www.docker.com/) takes away repetitive, mundane configuration tasks and is used throughout the development lifecycle for fast, easy and portable application development – desktop and cloud. Docker’s comprehensive end to end platform includes UIs, CLIs, APIs and security that are engineered to work together across the entire application delivery lifecycle.
  

##  Installing Project Dependencies

If you have docker and you want to run this project with **docker**, you can avoid this section and go to the next, docker does our lives easier, **docker is love docker is life**.

To install the project dependencies you need to create a new **virtual enviroment** with the next command:


    py -m venv venv

After you have to active the **virtual enviroment**

**Windows**

    .\venv\Scripts\activate

**Mac OS / Linux**

    source ./venv/bin/activate


And finally, you can install the dependencies running this command:

    pip install -r requeriments.txt

**Important Note**
We are writing and reading files from S3 and also writing (in my case) in a Postgres database in AWS RDS we need to download some important packages:

 - [Apache Hadoop AWS](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws)
  - [AWS SDK For Java Bundle](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle)
 -  [PostgreSQL JDBC Driver](https://jdbc.postgresql.org/download.html)

Apache Hadoop AWS has to have the same version used for Spark, AWS SDK for Java Bundle is an Apache Hadoop AWS dependency, so you have to download the correct version based on the Apache Hadoop AWS version. Finally, for the PostgreSQL JDBC Driver, I think that the version doesn't matter and doesn't have a conflict with the Hadoop or Spark version, so you only have to try to download the last version.

**Note:**
If you are using Spark 3.2.1 and Hadoop 3.3.1 I did a Script to download the packages easily, you only have to run the jars.py script and the script will download the jars in your jars folder into the Spark home

##  Setup Database in AWS RDS

You need to have previous knowledge in AWS RDS to set up the database, I'm my case I used Postgres, you have to enable the public access to the database and then modify the **VPC Security Group** to allow the access from your IP to the database.  I'm not sure but also you can use a local database to run this, you only have to change the .env variables to put your localhost instead of an AWS RDS host.

##  Setup S3

The S3 configuration is similar to the AWS RDS, but you have to create a new user in the IAM users, and in the AWS access type you have to select **programmatic access** because we are going to use an AWS Access Key and an AWS Secret Key in our .env file to configure the S3 access

## .env variables
You need to add (or modify the .env.example) with the needed information to run the application

- **AWS_ACCESS_KEY_ID=** The AWS access key generated for your user in the IAM roles to read and write data in S3      
- **AWS_SECRET_ACCESS_KEY=** The AWS secret access key generated for your user in the IAM roles to read and write data in S3     
-  **BUCKET_NAME=** Your bucket name.       
- **MUSEUMS_URL=** This URL doesn't have to change, is the URL for the Museums' information.    
- **CINEMAS_URL=** This URL doesn't have to change, is the URL for the CInemas' information. 
- **LIBRARIES_URL= ** This URL doesn't have to change, is the URL for the Libraries' information.  
- **DB_URL=** Your jdbc connection url with the next format: jdbc:postgresql://{**your_host**}/{**your_database**}    
- **DB_USER=** Your db user 
- **DB_PASSWORD=** Your db password

  

##  Running ETL Process
If you wanted to do this the hard way and install all the dependencies yourself, to run this project all you need to do is run the **main.py** script into the **src** folder
  

    python3 ./src/main.py

But if you wanted to do this in the easy and run the project with docker, you have to run only two commands to set up and run this project. Remember to put the **.env**  with the current information in the same level that the **Dockerfile** file before building the image. So to set up  the application with docker you have to run the next command to build the image and set up the project:

    docker build -t data_analytics_challenge .
  
  Once the image was built, you only have to run the next command to run the image in a new container:
  

    docker run data_analytics_challenge driver local:///opt/application/src/main.py

PD: Being sincerely when I started this project I wanted to load the information in redshift, but currently I'm having an issue with the Apache Redshift Community Driver and the S3 FileSystem, so looking in the GitHub it's currently an issue open with this error, so I created a branch to do the "redshift solution" in the future when the problem will be already fixed: [Fix URL](https://github.com/spark-redshift-community/spark-redshift/issues/103)

