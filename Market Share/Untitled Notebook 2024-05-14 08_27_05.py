# Databricks notebook source
pip install -U google_trends

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from google_trends import daily_trends, realtime_trends


# COMMAND ----------

daily_trends(date=None, country='US', language='en-US', timezone='-180')
#     #####Google daily search trends
    
#     date = YYYYMMDD (example: 20210810) - trends on a given date, interval: today - 30 days ago;
#     country = 'US', 'RU', etc.;
#     language = 'en-US', 'ru-RU', etc.;
#     timezone = timezone offset, example: GMT-7 == -7*60 = '-420'.
    

# trends today
today_trends = daily_trends(country='GB')
print(today_trends)

#trends on a given date, interval: today - 30 days ago.
date_trends = daily_trends(20240501)
print(date_trends)

# COMMAND ----------

today_trends
