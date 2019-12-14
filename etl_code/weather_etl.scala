// read from csv
val csvdf = spark.read.format("csv").option("header", "true").load("hdfs:///user/cy1505/proj/noaa_nyc_weather_2010-2019.csv")

csvdf.count()

// select some field
val df = csvdf.select("DATE", "AWND", "PRCP", "SNOW", "TMIN", "TMAX", "WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT09", "WT11","WT13","WT14","WT16","WT17","WT18","WT19","WT22")
val weather_type = Map("IS_FOG" -> List(1,2,22), "IS_THUNDER" -> List(3), "IS_HAIL" -> List(4,5), "IS_RIME" -> List(6), "IS_DUST" -> List(7), "IS_SMOKE" -> List(8), "IS_SNOW" -> List(9, 18), "IS_WIND" -> List(11), "IS_MIST" -> List(13), "IS_DRIZZLE" -> List(14, 16, 17, 19))
val weather_field_type = weather_type.map(p => p._1 -> p._2.map(x => if (x < 10) "WT0"+x else "WT"+x))

// unite some field into one
var new_df = df;
for ((weather, fields_list) <- weather_field_type) {
    new_df = new_df.withColumn(weather, when(fields_list.map(x => df(x).isNull).reduce((acc, x) => acc && x), 0).otherwise(1))
    fields_list.foreach(x => new_df = new_df.drop(x))
}

new_df = new_df.withColumn("IS_NORMAL", when(weather_field_type.map(x => new_df(x._1) === 0).reduce((acc, x) => acc && x), 0).otherwise(1))

// convert date

transform_date(date:String):String {
  val date_arr = date.split("-")
  List(date_arr(1), date_arr(2), date_arr(0)).mkString("/")
}
val udfconvert = udf(transform_date _)

val new_date_df = new_df.withColumn("DATE", udfconvert.apply($"DATE"))

// save
new_date_df.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").save("hdfs:///user/cy1505/proj/cleaned_weather_data")
