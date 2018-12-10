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

schema = StructType([
    StructField("created_at", StringType()),
    StructField("screename", StringType()),
    StructField("text", StringType()),
])

def improvreduce(alist):
    dict = {}
    for x in practice:
        for y in x:
            if y[0] in dict.keys:
                dict[y[0]] = dict[y[0]] + y[1]
            else:
                dict[y[0]] = y[1]

df = spark.read.csv("/user/joanette_rosario/text.csv", header=True, schema=schema)
df.printSchema()
df = df.withColumn('wordCount', f.size(f.split(f.col('text'), ' ')))
df.show()

keywordtext = df.select('text')

rdd = keywordtext.rdd.map(lambda x: x.text.split(" ") if x.text is not None else x.text)
rdd.collect()

rdd.map(lambda  row: [r if r is not None else "" for r in row])
rdd.collect()

def test(x):
    if(x is None):
        return "jajajaaja"
    if x[0] in stopwords:
        return (x,1)
    else:
        return (x, 0)

rdd2 = rdd.map(lambda w: map(lambda x: test(x[0]), list(w)))
rdd2.collect()
# rdd2 = rdd.map(lambda w: [(x,1) if x not in stopwords and x is not None and w else (x,0) for x in w] if w is not None else None)
# rdd2.collect()

# practice = [[("cat" ,1), ("potato", 1), ("clock",1 )],
#  [("cat", 1), ("potato", 1), ("clock", 1)],
#  [("cat", 1), ("potato", 1), ("clock", 1)],
#  [("cat", 1), ("potato", 1), ("clock", 1)]]
#
# list = [1, 2, 3, 4]
# product = reduce(lambda x, y: x*y, list)
#rdd3 = rdd2.map(lambda z: [reduce(lambda a,b: a[0] + b[0], z[1:]) if z is not None else None])
t_dict = {}
def add_todict(k):
    if k in t_dict.keys():
        t_dict[k]+=1
        print "here"
        return None
    else:
        t_dict[k] = 1
        print "here in else"
        return None

rdd3 = rdd2.map(lambda w: map(lambda x: x , list(w)))
rdd3.collect()
print(t_dict)
rdd3 =rdd2.reduceByKey(lambda a, b: a + b)
rdd3.top(2, key=lambda x: x[2])
rdd4 =rdd3.map(lambda (a, b): (b, a))
rdd5 = rdd4.sortByKey(ascending=False)
output = rdd5.collect()


#top hasthags
hashtags = rdd.filter(lambda w: '#' in w if w is not None else None).map(lambda x: (x, 1) if x is not None else (x,0))
hashtags2 = hashtags.reduceByKey(lambda x, y: x+y)
hashtags2.top(10, lambda t: t[1])
hashtags2.collect()
#top keywords
words = df.rdd.flatMap(list).flatMap(lambda line: line.split()).filter(!testWords.contains(line))
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
hashtags2 = hashtags.reduceByKey(lambda x, y: x+y)
hashtags2.top(10, lambda t: t[1])
#top participants

#use retweet select max of retweet count

#trump, flu, zika, diarrhea, Ebola, headache, measles

