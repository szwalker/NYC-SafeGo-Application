import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegressionModel


// load weather data from hdfs
val weather = spark.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema", "true").load("hdfs:///user/cy1505/proj/today_weather.csv")
val weather_arr = weather.as[(Double, Double, Double, Double, Double,Double)].rdd.first
val wind = weather_arr._1;
val prcp = weather_arr._2;
val snow = weather_arr._3;
val tmin = weather_arr._4;
val tmax = weather_arr._5;
val normal = weather_arr._6;

// load nycmap
val nycmap = spark.read.format("parquet").option("header","true").option("inferSchema", "true").load("hdfs:///user/cy1505/proj/etl_manhattan_map.parquet")
val mapdf = nycmap.withColumn("loc", explode(col("polygons"))).select("PHYSICALID", "loc.*").withColumnRenamed("_1","lat").withColumnRenamed("_2","lng")

// load model
val model = LogisticRegressionModel.load("hdfs:///user/cy1505/proj/crime.model")

// transformer of data to predefined format
val assembler = new VectorAssembler().setInputCols(Array("lat", "lng", "homeless", "drinking", "streetlight", "wind", "prcp", "snow", "tmin", "tmax", "normal")).setOutputCol("features")

// init some constant and variable
val newColMap = Map("wind"->wind, "prcp" -> prcp, "snow"->snow,"tmin"->tmin, "tmax"->tmax,"normal"->normal, "homeless"->0, "drinking"->0, "streetlight"->0)
val newColNames = Seq("homeless","drinking","streetlight","wind","prcp","snow","tmin","tmax","normal","normal")

// pre defined constant
val coefficient = Array(1, 2, 5)
def calcCrimeIndex = udf( (x:DenseVector) => coefficient(0)*x(0)+coefficient(1)*x(1)+coefficient(2)*x(2))

val df = newColNames.foldLeft(mapdf)((acc, col) => {acc.withColumn(col,lit(newColMap(col)).as("Double"))})
val topred = assembler.transform(df).select("PHYSICALID","features")
val myprob = model.transform(topred).select("PHYSICALID","myProbability")
val prediction = myprob.withColumn("crimeIndex", calcCrimeIndex($"myProbability")).drop("myProbability")
prediction.write.format("csv").save("hdfs:///user/cy1505/proj/prediction.csv")
