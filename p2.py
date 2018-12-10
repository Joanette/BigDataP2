from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql.functions import split, explode
from functools import reduce
import collections
spark = SparkSession \
    .builder \
    .appName("Project #2") \
    .getOrCreate()
# spark is an existing SparkSession
stopwords = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as",
             "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't",
             "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down",
             "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't",
             "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself",
             "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
             "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off",
             "on", "once", "only", "or", "other", "ought", "our", "ours    ourselves", "out", "over", "own", "same",
             "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that",
             "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they",
             "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up",
             "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's",
             "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with",
             "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
             "yourselves"];
wordsToFilter = ["trump", "flu", "zika", "diarrhea", "Ebola", "headache", "measles"]
schema = StructType([
    StructField("created_at", StringType()),
    StructField("screename", StringType()),
    StructField("text", StringType()),
    StructField("retweet_count", IntegerType())
])

def removePunctuation(text):
    text=text.lower().strip()
    return text

def wordCount(wordListRDD):
    return wordListRDD.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)

df = spark.read.csv("/user/joanette_rosario/1207.csv", header=True, schema=schema)
df.printSchema()
df = df.withColumn('wordCount', f.size(f.split(f.col('text'), ' ')))
df.show()
#top 10 keywords
tweetRDD = (sc.textFile("textfile.txt", 8).map(removePunctuation))
tweetWordsRDD = tweetRDD.flatMap(lambda x: x.split(" "))
WordsRDD = tweetWordsRDD.filter(lambda x: x != "" and x not in stopwords)
RDD = wordCount(WordsRDD)
RDD.top(10, key=lambda x: x[1])
#top 10 hashtags
tweetRDD = (sc.textFile("textfile.txt", 8).map(removePunctuation))
tweetWordsRDD = tweetRDD.flatMap(lambda x: x.split(" "))
WordsRDD = tweetWordsRDD.filter(lambda x: "#" in x)
RDD = wordCount(WordsRDD)
RDD.top(10, key=lambda x: x[1])
#trump, flu, zika, diarrhea, Ebola, headache, measles
tweetRDD = (sc.textFile("textfile.txt", 8).map(removePunctuation))
tweetWordsRDD = tweetRDD.flatMap(lambda x: x.split(" "))
WordsRDD = tweetWordsRDD.filter(lambda x: x in wordsToFilter)
RDD = wordCount(WordsRDD)
RDD.top(10, key=lambda x: x[1])
#top participants screennames that have posted the most
df.createOrReplaceTempView("tweets")
sqlDf = spark.sql("SELECT screename, count(*) as NUM FROM tweets GROUP BY screename ORDER BY NUM ASC LIMIT 10")
