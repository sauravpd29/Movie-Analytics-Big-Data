#Generating count of movies corresponding to each genre of the movies and displaying it in descending order.
movies.groupby("genre").count().orderBy("count", ascending=False).show()

#Generating count of movies corresponding to each country and displaying them in descending order.
movies.groupby("country").count().orderBy("count", ascending=False).show()

#Generating count of movies corresponding to each production company and displaying it in descending order.
movies.groupby("production_company").count().orderBy("count", ascending=False).show()

print (sorted(ratings.select('rating').distinct().rdd.map(lambda r: r[0]).collect()))


movies.createOrReplaceTempView("movie")

#Showing all the comedy movies released by Netflix in 2019.
spark.sql("select title,year,genre,production_company,country from movie where year=2019 and genre='Comedy' and production_company='Netflix'").show()

#Finding movies which has the max number of reviews from users.
spark.sql("select title,reviews_from_users from movie where reviews_from_users in (select max(reviews_from_users) from movie)").show()

#Finding Comedy movies which have the max number of reviews from critics and are released in the year 2019.
spark.sql("select title,reviews_from_critics from movie where reviews_from_critics in (select max(reviews_from_critics) from movie where year=2019 and genre='Comedy')").show()

#Finding the longest Hindi movie released in India in 2019.
spark.sql("select title,duration from movie where duration in (select max(duration) from movie where year=2019 and language='Hindi' and country='India')").show()

#Finding those movies which are written and directed by the same person and released in the year 2020.
spark.sql("select title,director,writer from movie where writer=director and year=2020").show()
