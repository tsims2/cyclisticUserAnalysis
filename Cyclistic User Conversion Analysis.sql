-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Cyclistic Bike Sharing User Analysis
-- MAGIC ##Google Data Analytics Capstone

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This analysis was completed as my capstone for the Goodle Data Analytics Specilization. It uses public made available by Motivate International Inc. and is not representative of an actual company.
-- MAGIC 
-- MAGIC For this analysis I followed the following framework.
-- MAGIC - Ask
-- MAGIC - Prepare
-- MAGIC - Process
-- MAGIC - Analyze
-- MAGIC - Share
-- MAGIC - Act

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Scenorio
-- MAGIC I am a junior data analyst working in the marketing analyst team at Cyclistic, a bike-share company in Chicago. The director
-- MAGIC of marketing believes the company’s future success depends on maximizing the number of annual memberships. Therefore,
-- MAGIC my team wants to understand how casual riders and annual members use Cyclistic bikes differently. From these insights,
-- MAGIC my team will design a new marketing strategy to convert casual riders into annual members. But first, Cyclistic executives
-- MAGIC must approve my recommendations, so they must be backed up with compelling data insights and professional data
-- MAGIC visualizations.
-- MAGIC 
-- MAGIC ###Specific Question
-- MAGIC How do annual members and casual riders use Cyclistic bikes differently?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ###Ask
-- MAGIC During this phase of the project I want to get a good understanding of what is being asked of me, where I can find the required data, and what is expected of the outputs.
-- MAGIC 
-- MAGIC Here are questions I would like to be able to ask the director:
-- MAGIC 1. Where is the data associated with user activity stored?
-- MAGIC 2. Has another team worked on this problem before?
-- MAGIC 3. How do current memberships work?
-- MAGIC 
-- MAGIC In this case study I cannot speak with the director but I am given some insights that will hopefully allow me to get started. The relevant data is stored in an open database. I am not permitted access to data related to transactions and was informed I would need to complete this anlysis without it. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Prepare
-- MAGIC These are the steps I followed in downloading and storing my data:
-- MAGIC 1. I saved the last 12 months of user ddata available in the database to a folder on my desktop for this project
-- MAGIC 2. I saved each month in its on folder following this naming convention monthYear
-- MAGIC 3. I uploaded each file to my DataBricks File directory under the same filing conventions
-- MAGIC 4. I unioned all 12 months of user data to create a table called userData12Months
-- MAGIC 
-- MAGIC Once I had the userData12Months table I skimmed over it and developed more questions I would like to ask the director:
-- MAGIC 1. Is there any insight on why there are null values associated with location pick up or drop off
-- MAGIC 2. Is there any data connecting the ride_id to a specific user
-- MAGIC 3. What are the different bike types used for?
-- MAGIC 
-- MAGIC Because of the nature of this assignment I could not get more clarity on the project. I assumed that where a person dropped off or picked up their bike was not an important attribute to the analysis I wanted to conduct.

-- COMMAND ----------

DROP DATABASE IF EXISTS CyclisticUserStudy;
CREATE DATABASE CyclisticUserStudy;
USE CyclisticUserStudy;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW jan2021
USING parquet
OPTIONS (
  path "/user/hive/warehouse/jan2021"
);
CREATE OR REPLACE TEMPORARY VIEW feb2021
USING parquet
OPTIONS (
  path "/user/hive/warehouse/feb2021"
);
CREATE OR REPLACE TEMPORARY VIEW mar2021
USING parquet
OPTIONS (
  path "/user/hive/warehouse/mar2021"
);
CREATE OR REPLACE TEMPORARY VIEW apr2021
USING parquet
OPTIONS (
  path "/user/hive/warehouse/apr2021"
);CREATE OR REPLACE TEMPORARY VIEW may2021
USING parquet
OPTIONS (
  path "/user/hive/warehouse/may2021"
);
CREATE OR REPLACE TEMPORARY VIEW jun2021
USING parquet
OPTIONS (
  path "/user/hive/warehouse/jun2021"
);
CREATE OR REPLACE TEMPORARY VIEW jul2021
USING parquet
OPTIONS (
  path "/user/hive/warehouse/jul2021"
);
CREATE OR REPLACE TEMPORARY VIEW aug2021
USING parquet
OPTIONS (
  path "/user/hive/warehouse/aug2021"
);
CREATE OR REPLACE TEMPORARY VIEW sep2021
USING parquet
OPTIONS (
  path "/user/hive/warehouse/sep2021"
);
CREATE OR REPLACE TEMPORARY VIEW oct2020
USING parquet
OPTIONS (
  path "/user/hive/warehouse/oct2020"
);
CREATE OR REPLACE TEMPORARY VIEW nov2020
USING parquet
OPTIONS (
  path "/user/hive/warehouse/nov2020"
);
CREATE OR REPLACE TEMPORARY VIEW dec2020
USING parquet
OPTIONS (
  path "/user/hive/warehouse/dec2020"
);

-- COMMAND ----------

CREATE OR REPLACE TABLE userData12Months
USING DELTA
AS(   SELECT * FROM jan2021
UNION SELECT * FROM feb2021
UNION SELECT * FROM mar2021
UNION SELECT * FROM apr2021
UNION SELECT * FROM may2021
UNION SELECT * FROM jun2021
UNION SELECT * FROM jul2021
UNION SELECT * FROM aug2021
UNION SELECT * FROM sep2021
UNION SELECT * FROM oct2020
UNION SELECT * FROM nov2020
UNION SELECT * FROM dec2020)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Process
-- MAGIC I created a new table called curserDataYearly with the following columns:
-- MAGIC - ride_id
-- MAGIC - rideable_type
-- MAGIC - started_at
-- MAGIC - ended_at
-- MAGIC - year_started
-- MAGIC - month_started
-- MAGIC - day_started
-- MAGIC - ride_length_hours
-- MAGIC - ride_length_minutes,
-- MAGIC - member_casual
-- MAGIC 
-- MAGIC I believed these columns would help me develop meaningful analysis. 

-- COMMAND ----------

DROP TABLE IF EXISTS cuserDataYearly;
CREATE TABLE cuserDataYearly
USING DELTA
AS(
SELECT 
  ride_id,
  rideable_type,
  started_at,
  ended_at,
  DATE_FORMAT(started_at, 'yyyy') AS year_started,
  DATE_FORMAT(started_at, 'MMMM') AS month_started,
  DATE_FORMAT(started_at, 'EEEE') AS day_started,
  CASE   WHEN HOUR(started_at) > HOUR(ended_at) THEN ROUND(((HOUR(ended_at)*60*60 +   MINUTE(ended_at)*60 +   SECOND(ended_at) + (24*60*60)) - 
                                                            (HOUR(started_at)*60*60 + MINUTE(started_at)*60 + SECOND(started_at)))/60/60,2)
                                                ELSE ROUND(((HOUR(ended_at)*60*60 +   MINUTE(ended_at)*60 +   SECOND(ended_at)) - 
                                                            (HOUR(started_at)*60*60 + MINUTE(started_at)*60 + SECOND(started_at)))/60/60,2)
                                                END AS ride_length_hours, 
  CASE   WHEN HOUR(started_at) > HOUR(ended_at) THEN ROUND(((HOUR(ended_at)*60*60 +   MINUTE(ended_at)*60 +   SECOND(ended_at) + (24*60*60)) - 
                                                            (HOUR(started_at)*60*60 + MINUTE(started_at)*60 + SECOND(started_at)))/60,2)
                                                ELSE ROUND(((HOUR(ended_at)*60*60 +   MINUTE(ended_at)*60 +   SECOND(ended_at)) - 
                                                            (HOUR(started_at)*60*60 + MINUTE(started_at)*60 + SECOND(started_at)))/60,2)
                                                END AS ride_length_minutes,                                             
  member_casual
FROM 
  userData12Months)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After I created curserDataYearly, I checked for null values. Once I was certain that my data was clean and reliable, I downloaded 1000 random data points for deeper analysis usisng R. 

-- COMMAND ----------

SELECT
  *
FROM
  cuserDataYearly
ORDER BY 
  RAND()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Analysis
-- MAGIC %md
-- MAGIC Analysis was conducted using R in RStudio. To see the markdown file for the analysis please see my linkedIn page. 
-- MAGIC 
-- MAGIC There where three major findings with the analysis:
-- MAGIC 1. Members make up the majority of rides during the weekday while casual riders are more likely to use our services during the weekend
-- MAGIC 2. The Cyclistic platform is much more active during warmer months compared to codler months
-- MAGIC 3. Casual members are much more likely to engage in long rides (longer than 1 hour)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Share
-- MAGIC 
-- MAGIC To share my data and findings with the executive team at Cyclistic, I have created these markdown files. These files detail my approach, assumptions and code used. 
-- MAGIC 
-- MAGIC If this was a project for an actual company, I would save the two markdown files as one report and save it to the company drive.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Act
-- MAGIC 
-- MAGIC I would recommend the following strategies to help Cyclistic increase their profitability.
-- MAGIC 1. Create promotions that highlight the benefits of using the Cyclistic platform for everyday traveling needs
-- MAGIC 2. Create material that highlights summer activities and events that could be better enjoyed with the Cyclistic platform
-- MAGIC 3. Create material that highlights the benefits of ending sessions when bikes are not in use
