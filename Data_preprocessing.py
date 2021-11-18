movie=spark.read.option("header","true").option("inferSchema","true").csv("file:///home/saurav/movies/movie.csv")
imdb_movies=spark.read.option("header","true").option("inferSchema","true").csv("file:///home/saurav/movies/IMDb_movies.csv")
links=spark.read.option("header","true").option("inferSchema","true").csv("file:///home/saurav/movies/links.csv")
ratings=spark.read.option("header","true").option("inferSchema","true").csv("file:///home/saurav/movies/ratings.csv")

m1=movie.join(links,on=['movieId'],how='inner').drop(links['tmdbId'])
m2=imdb_movies.join(m1,on=['imdbId'],how='inner').drop(m1['title']).drop(m1['genres'])
movies=m2.drop("original_title","votes","budget","usa_gross_income","worlwide_gross_income","metascore")
from pyspark.sql.types import DateType
movies=movies.withColumn("avg_vote",movies['avg_vote'].cast('double'))
movies=movies.withColumn("year",movies['year'].cast('int'))
movies=movies.withColumn("duration",movies['duration'].cast('int'))
movies=movies.withColumn("reviews_from_users",movies['reviews_from_users'].cast('int'))
movies=movies.withColumn("reviews_from_critics",movies['reviews_from_critics'].cast('int'))
movies=movies.dropna()

movies.printSchema()
ratings.printSchema()

