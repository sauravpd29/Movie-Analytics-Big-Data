n = ratings.select("rating").count()
num_users = ratings.select("userId").distinct().count()                     
num_movies = ratings.select("movieId").distinct().count()                   
d = num_users * num_movies                                        
sparsity = (1.0 - (n *1.0)/d)*100
print("The ratings dataframe is: ", "%.2f" % sparsity + "% empty.")

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
(training, test) = ratings.randomSplit([0.8, 0.2])
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",coldStartStrategy="drop")
model = als.fit(training)
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

predictions.show()

user_history = training.filter(training['userId']==20
user_history.show()

user_suggest = test.filter(training['userId']==20).select(['movieId','userId'])
user_suggest.show()

user_offer = model.transform(user_suggest)
user_offer.orderBy('prediction',ascending=False).show()

user_recommendation=user_offer.join(movies,on=['movieId'],how='inner').select(['userId','movieId','title','year','director','actors','genre','language','country'])
user_recommendation.show()
