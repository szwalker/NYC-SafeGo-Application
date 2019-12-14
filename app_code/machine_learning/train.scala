import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.VectorAssembler

val data = spark.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema", "true").load("hdfs:///user/cy1505/proj/train_data.csv")
val assembler = new VectorAssembler().setInputCols(Array("lat", "lng", "homeless", "drinking", "streetlight", "wind", "prcp", "snow", "tmin", "tmax", "normal")).setOutputCol("features")
val output = assembler.transform(data).withColumnRenamed("crime_type","label")
val train = output.select("features","label")

val lr = new LogisticRegression()
val model = lr.fit(train, paramMapCombined)
val paramMap = ParamMap(lr.maxIter -> 20).put(lr.maxIter, 30).put(lr.regParam -> 0.1, lr.threshold -> 0.55)
val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
val paramMapCombined = paramMap ++ paramMap2
val model = lr.fit(train, paramMapCombined)
model.transform(train).select("features", "label", "myProbability", "prediction").show(5)
model.summary.accuracy
model.transform(train).select("prediction").show(20)
model.transform(train).select("myProbability").rdd.take(5)
model.save("hdfs:///user/cy1505/crime.model")
