# Movie-Analytics-Big-Data

#### IMDB Dataset                     
The movies dataset includes 85,855 movies with attributes such as movie description, average rating, number of votes, genre, etc.
Attributes
1. imdbId: title ID on IMDb(integer)
2. Title:title name(string)
3. orginal_title:original title name(string)
4. year:year of release(string)
5. date_published:date of release(string)
6. Genre:movie genre(string)
7. Duration:duration in min(integer)
8. Country:movie country(string)
9. Language:Movie language(string)
10. director:Director name(string)
11. Writer:Writer name(string)
12. Production_Company:production company(string)
13. Actors:actor names(string)
14. Description:plot description(string)
15. Avg_vote:average votes(string)
16. Reviews_from_users:no of review from user(string)
17. Reviews_from_users:no of review from critics(string)

#### Movielens Dataset 
The datasets describe ratings and free-text tagging activities from MovieLens
rating.csv that contains ratings of movies by users: userId : movieId : rating : timestamp
movie.csv that contains movie information: movieId : title : genres

The main aim of this project is to demonstrate the movie analytics using spark technology. In the MLR model, we are predicting the average votes based on other attributes of IMDb dataset (director, writer, genre, duration, year).

Collaborative filtering is commonly used for recommender systems. These techniques aim to fill in the missing entries of a user-item association matrix. In collaborative filtering, the users and movies are described by a small set of latent factors. Here we are using ALS model to find the latent factors and to suggest some movies to user 20 based on our model.

