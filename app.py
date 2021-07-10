from flask import Flask
import os
from random import randrange
import json
import re
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lower, regexp_replace, col, desc, split, size, explode

app = Flask(__name__)

@app.route('/')
def main():
    '''Retrieve list of file names'''
    fileList = [file for file in os.listdir("./data")]

    data = []

    '''Get random "n" number and iterate through first n files'''
    ind = randrange(len(fileList))
    for i in range(ind):
        # fileName = '/Users/pshin/juneProj/data/'+fileList[i]
        fileName = 'data/'+fileList[i]
        fileObj = open(fileName, 'r')
        objs = fileObj.readlines()

        '''Convert into dict and load each object in file into data list'''
        for obj in objs:
            data.append(json.loads(obj))

    '''Create Spark Session'''
    spark = SparkSession.builder.appName('wordCounter').master('local').getOrCreate()

    '''Create DataFrame from list of dictionaries'''
    df = df2 = spark.createDataFrame([Row(**i) for i in data])

    '''Get list of top 10 most frequent cities'''
    topCities = df.filter(df.city != 'None').groupBy('city').count().orderBy(col('count').desc()).rdd.map(lambda x: x.city).collect()[:10]

    '''Filter out cities outside top 10 cities'''
    df = df2.filter(df2.city.isin(topCities))

    '''Filter out non alphanumeric values from text field (but keep spaces)'''
    df = df.withColumn('text', regexp_replace(col('text'), '[^0-9a-zA-Z ]+', ''))

    '''Convert text field to lowercase'''
    df = df.withColumn('text', lower(col('text')))

    '''Separate each word of text field into its own row'''
    df = df.select(df.city, explode(split(df.text, ' ')).alias('word')).groupBy('city', 'word').count()

    '''Process Dataframe to pull out top 10 most frequent words per city and their corresponding counts'''
    output = []

    for city in topCities:
        cityDf = df.filter(df.city == city)
        
        rows = cityDf.orderBy(col('count').desc()).limit(10).collect()
        
        words = []
        
        for row in rows:
            words.append({row['word']:row['count']})
            
        output.append({city:words})
        
    return json.dumps(output)