from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel

categorical_columns= ['director', 'writer', 'genre', 'duration', 'year']

indexers = [
             StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c)).setHandleInvalid("skip")
             for c in categorical_columns
 ]
encoders = [OneHotEncoder(dropLast=False,inputCol=indexer.getOutputCol(),
             outputCol="{0}_encoded".format(indexer.getOutputCol())) 
             for indexer in indexers
 ]
assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders],outputCol="features").setHandleInvalid("skip")

pipeline = Pipeline(stages=indexers + encoders+[assembler])
model=pipeline.fit(movies)
transformed = model.transform(movies)                                   
transformed.printSchema()

final = transformed.select("features", "avg_vote")
final.printSchema()
final.show()

lr_train,lr_test = final.randomSplit([0.8, 0.2])
lr = LinearRegression(labelCol="avg_vote",fitIntercept=True,maxIter=100,regParam=0.02,elasticNetParam=0.02)
lrModel = lr.fit(lr_train)

trainingSummary = lrModel.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)
print("Intercept: %s" % str(lrModel.intercept))
print("numIterations: %d" % trainingSummary.totalIterations)
trainingSummary.predictions.printSchema()
trainingSummary.predictions.show()
training_residuals = trainingSummary.residuals

prediction_and_labels = lrModel.evaluate(final)
print("RMSE: %f" % prediction_and_labels.rootMeanSquaredError)
print("r2: %f" % prediction_and_labels.r2)
prediction_and_labels.predictions.show(50)

test_predict = lrModel.transform(lr_test)
test_result = lrModel.evaluate(lr_test)
print("RMSE: %f" % test_result.rootMeanSquaredError)
print("r2: %f" % test_result.r2)
actual_and_predicted = test_predict.select("avg_vote", "prediction")
actual_and_predicted.show()
