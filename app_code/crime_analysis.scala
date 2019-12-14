// The frequency of each crime type
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//get the word count of crime type
val crimeAnalyze = crimeFile.map(line => (line(2),1)).countByKey
val crimeRes = sc.parallelize(crimeAnalyze.toList)
crimeRes.sortBy(_._2,ascending=false).saveAsTextFile("/user/yl6127/project/crimeTypeCount/")

// The frequency of crime level
val crimeRDD = sc.textFile("/user/yl6127/project/crime")
val crimeLines = crimeRDD.map(L=>L.split(","))
//get the word count for crime level field
crimeLines.map(L=>(L(3),1)).countByKey

// The frequency of crime in different boroughs
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//get the word count for borough field
val crimeBoro = crimeFile.map(line => (line(4),1)).countByKey

// The frequency of crime in different hours
val crimeHour = crimeFile.map(line => (line(1),1)).countByKey

// The total number of each 311 complaints (in descending order)
val TOO = sc.textFile("/user/jl8456/BDAD/project/311_data_cleaned")

TOO.filter(L=>L.contains("street light")).count

TOO.filter(L=>L.contains("homeless encampment")).count

TOO.filter(L=>L.contains("drinking in public")).count

//average crime per month
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
// get the total crime count per month
val crimeRDD1 = crimeFile.map(line => (line(0).split("/")(0), 1)).countByKey
val crimeRes = sc.parallelize(crimeRDD1.toList)
//save the result to dataframe
val crimeDF = spark.createDataFrame(crimeRes).toDF("Month","Count")
val df = crimeDF.sort("Month")

df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/crimeCountByMonth.csv")

import org.apache.spark.sql.types._
val customSchema = StructType(Array(
  StructField("date", StringType, true),
  StructField("time", IntegerType, true),
  StructField("crime_type", StringType, true),
  StructField("crime_level", StringType, true),
  StructField("borough", StringType, true),
  StructField("latitude", DoubleType, true),
  StructField("longitude", DoubleType, true))
)
val crime_df = sqlContext.read.format("csv").schema(customSchema).load("/user/yl6127/project/crime/*")

//get the count of crime types per month
/*
average felony crime per month
*/

//read in file and get the certain crime type
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.toLowerCase).filter(line => (line.contains("felony"))).map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
// get the count per month
val crimeRDD1 = crimeFile.map(line => (line(0).split("/")(0), 1)).countByKey
val crimeRes = sc.parallelize(crimeRDD1.toList)
//save to dataframe
val crimeDF = spark.createDataFrame(crimeRes).toDF("Month","Count")
val df = crimeDF.sort("Month")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/felonyCountByMonth")


//average misdemeanor crime per month
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.toLowerCase).filter(line => (line.contains("misdemeanor"))).map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
val crimeRDD1 = crimeFile.map(line => (line(0).split("/")(0), 1)).countByKey
val crimeRes = sc.parallelize(crimeRDD1.toList)
val crimeDF = spark.createDataFrame(crimeRes).toDF("Month","Count")
val df = crimeDF.sort("Month")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/misdemeanorCountByMonth")


//average violation crime per month
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.toLowerCase).filter(line => (line.contains("violation"))).map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
val crimeRDD1 = crimeFile.map(line => (line(0).split("/")(0), 1)).countByKey
val crimeRes = sc.parallelize(crimeRDD1.toList)
val crimeDF = spark.createDataFrame(crimeRes).toDF("Month","Count")
val df = crimeDF.sort("Month")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/violationCountByMonth")

//average temperature
val weatherFile = sc.textFile("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv").filter(line=>(!line.contains("AWND"))).map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//take the monthly total temperature by summing up the max_temp and min_temp
val weatherRdd1 = weatherFile.map(line => (line(0).split("/")(0), (line(4).toFloat + line(5).toFloat)/2)).groupByKey

for(m <- 1 to 13) {
var count = weatherFile.filter(line => (line(0).split("/")(0).toInt == m)).count
// divide the total average temperature by the crime count per month
var weatherRdd2 = weatherRdd1.map(line => (line._1,line._2.toList.sum/line._2.toList.length))
}

val df = weatherDF.sort("Month")

val weatherDF = spark.createDataFrame(weatherRdd2).toDF("Month","AvgTemp")

df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/tempAvgByMonth.csv")


val weatherRDD = spark.read.format("com.databricks.spark.csv").option("header","true").load("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv")


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
import sqlContext._
import sqlContext.implicits._
weatherRDD.registerTempTable("weather")


//count the crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//count the crime record per day
val crimeByDay = crimeFile.map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
//order the date in ascending order
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/crimeCountByDay")

//count the felony crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.toLowerCase).map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//count the felony records per day
val crimeByDay = crimeFile.filter(line => (line(3).contains("felony"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/felonyCountByDay")

//count the misdemeanor crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.toLowerCase).map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//count the crime record per day
val crimeByDay = crimeFile.filter(line => (line(3).contains("misdemeanor"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
//order the date in ascending order
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/misdemeanorCountByDay")

//count the violation crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.toLowerCase).map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
val crimeByDay = crimeFile.filter(line => (line(3).contains("violation"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/violationCountByDay")

//count the robbery crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.toLowerCase).map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//count the crime record per day
val crimeByDay = crimeFile.filter(line => (line(2).contains("robbery"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/robberyCountByDay")


//count the PETIT LARCENY crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//count the crime record per day
val crimeByDay = crimeFile.filter(line => (line(2).contains("PETIT LARCENY"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/petitlarcenyCountByDay")

//count the GRAND LARCENY crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//count the crime record per day
val crimeByDay = crimeFile.filter(line => (line(2).contains("GRAND LARCENY"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/grandlarcenyCountByDay")

//count the HARRASSMENT2 crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//count the crime record per day
val crimeByDay = crimeFile.filter(line => (line(2).contains("HARRASSMENT 2"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/harrassment2CountByDay")

//count the ASSAULT 3 & RELATED OFFENSES crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//count the crime record per day
val crimeByDay = crimeFile.filter(line => (line(2).contains("ASSAULT 3 & RELATED OFFENSES"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/assault3CountByDay")

//count the CRIMINAL MISCHIEF & RELATED OF crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
//count the crime record per day
val crimeByDay = crimeFile.filter(line => (line(2).contains("CRIMINAL MISCHIEF & RELATED OF"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/mischiefCountByDay")

//count the DANGEROUS DRUGS crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
val crimeByDay = crimeFile.filter(line => (line(2).contains("DANGEROUS DRUGS"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/drugsCountByDay")


//count the OFF. AGNST PUB ORD SENSBLTY crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
val crimeByDay = crimeFile.filter(line => (line(2).contains("OFF. AGNST PUB ORD SENSBLTY"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/pubCountByDay")

//count the FELONY ASSAULT crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
val crimeByDay = crimeFile.filter(line => (line(2).contains("FELONY ASSAULT"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/felonyAssaultCountByDay")

//count the BURGLARY crime by day
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
val crimeByDay = crimeFile.filter(line => (line(2).contains("BURGLARY"))).map(line => (line(0),1)).countByKey
val crimeRes = sc.parallelize(crimeByDay.toList)
crimeRes.map(line => ((line._1.split("/")(2)+"/"+line._1.split("/")(0)+"/"+line._1.split("/")(1)), line._2)).sortBy(_._1).map(line => ((line._1.split("/")(1)+"/"+line._1.split("/")(2)+"/"+line._1.split("/")(0)), line._2)).map(line => line.toString()).map(line => line.substring(1,line.length-1)).saveAsTextFile("/user/yl6127/project/analysis/burglaryCountByDay")

/*
Join the crime count per day with precipitation record per day, and then aggregate the number of crime count per each precipitation level
*/
//prcp vs OFF. AGNST PUB ORD SENSBLTY
val weatherFile = sc.textFile("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv").filter(line=>(!line.contains("AWND"))).map(line => line.replace(",,",",0,")).map(line => line.split(","))
val prcpRDD = weatherFile.map(line => (line(0), line(2)))
val pub = sc.textFile("/user/yl6127/project/analysis/pubCountByDay/").map(line => (line.split(","))).map(line => (line(0), line(1)))
val combined = prcpRDD.join(pub).map(line => (line._2._1, line._2._2.toInt)).groupByKey
val sum = combined.map(line => (line._1.toFloat,line._2.toList.sum)).sortBy(_._1)
val prcpPubDF = spark.createDataFrame(sum).toDF("PRCP","Count")
prcpPubDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/prcpVSPub.csv")

//prcp vs felonyAssault
val weatherFile = sc.textFile("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv").filter(line=>(!line.contains("AWND"))).map(line => line.replace(",,",",0,")).map(line => line.split(","))
val prcpRDD = weatherFile.map(line => (line(0), line(2)))
val felonyAssault = sc.textFile("/user/yl6127/project/analysis/felonyAssaultCountByDay/").map(line => (line.split(","))).map(line => (line(0), line(1)))
val combined = prcpRDD.join(felonyAssault).map(line => (line._2._1, line._2._2.toInt)).groupByKey
val sum = combined.map(line => (line._1.toFloat,line._2.toList.sum)).sortBy(_._1)
val prcpFelonyAssaultDF = spark.createDataFrame(sum).toDF("PRCP","Count")
prcpFelonyAssaultDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/prcpVSFelonyAssault.csv")

//prcp vs burglary
val weatherFile = sc.textFile("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv").filter(line=>(!line.contains("AWND"))).map(line => line.replace(",,",",0,")).map(line => line.split(","))
val prcpRDD = weatherFile.map(line => (line(0), line(2)))
val burglary = sc.textFile("/user/yl6127/project/analysis/burglaryCountByDay/").map(line => (line.split(","))).map(line => (line(0), line(1)))
val combined = prcpRDD.join(burglary).map(line => (line._2._1, line._2._2.toInt)).groupByKey
val sum = combined.map(line => (line._1.toFloat,line._2.toList.sum)).sortBy(_._1)
val prcpBurglaryDF = spark.createDataFrame(sum).toDF("PRCP","Count")
prcpBurglaryDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/prcpVSBurglary.csv")

//prcp vs robbery
val weatherFile = sc.textFile("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv").filter(line=>(!line.contains("AWND"))).map(line => line.replace(",,",",0,")).map(line => line.split(","))
val prcpRDD = weatherFile.map(line => (line(0), line(2)))
val robbery = sc.textFile("/user/yl6127/project/analysis/robberyCountByDay/").map(line => (line.split(","))).map(line => (line(0), line(1)))
val combined = prcpRDD.join(robbery).map(line => (line._2._1, line._2._2.toInt)).groupByKey
val sum = combined.map(line => (line._1.toFloat,line._2.toList.sum)).sortBy(_._1)
val prcpRobberyDF = spark.createDataFrame(sum).toDF("PRCP","Count")
prcpRobberyDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/prcpVSRobbery.csv")

//prcp vs general crime
val weatherFile = sc.textFile("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv").filter(line=>(!line.contains("AWND"))).map(line => line.replace(",,",",0,")).map(line => line.split(","))
val prcpRDD = weatherFile.map(line => (line(0), line(2)))
val generalCrime = sc.textFile("/user/yl6127/project/analysis/crimeCountByDay/").map(line => (line.split(","))).map(line => (line(0), line(1)))
val combined = prcpRDD.join(generalCrime).map(line => (line._2._1, line._2._2.toInt)).groupByKey
val sum = combined.map(line => (line._1.toFloat,line._2.toList.sum)).sortBy(_._1)
val prcpCrimeDF = spark.createDataFrame(sum).toDF("PRCP","Count")
prcpCrimeDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/prcpVSCrime.csv")


//prcp vs felony
val weatherFile = sc.textFile("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv").filter(line=>(!line.contains("AWND"))).map(line => line.replace(",,",",0,")).map(line => line.split(","))
val prcpRDD = weatherFile.map(line => (line(0), line(2)))
val felony = sc.textFile("/user/yl6127/project/analysis/felonyCountByDay/").map(line => (line.split(","))).map(line => (line(0), line(1)))
val combined = prcpRDD.join(felony).map(line => (line._2._1, line._2._2.toInt)).groupByKey
val sum = combined.map(line => (line._1.toFloat,line._2.toList.sum)).sortBy(_._1)
val prcpFelonyDF = spark.createDataFrame(sum).toDF("PRCP","Count")
prcpFelonyDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/prcpVSFelony.csv")

//prcp vs misdemeanor
val weatherFile = sc.textFile("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv").filter(line=>(!line.contains("AWND"))).map(line => line.replace(",,",",0,")).map(line => line.split(","))
val prcpRDD = weatherFile.map(line => (line(0), line(2)))
val misdemeanor = sc.textFile("/user/yl6127/project/analysis/misdemeanorCountByDay/").map(line => (line.split(","))).map(line => (line(0), line(1)))
val combined = prcpRDD.join(misdemeanor).map(line => (line._2._1, line._2._2.toInt)).groupByKey
val sum = combined.map(line => (line._1.toFloat,line._2.toList.sum)).sortBy(_._1)
val prcpMisDF = spark.createDataFrame(sum).toDF("PRCP","Count")
prcpMisDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/prcpVSMisdemeanor.csv")

//prcp vs violation
val weatherFile = sc.textFile("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv").filter(line=>(!line.contains("AWND"))).map(line => line.replace(",,",",0,")).map(line => line.split(","))
val prcpRDD = weatherFile.map(line => (line(0), line(2)))
val violation = sc.textFile("/user/yl6127/project/analysis/violationCountByDay/").map(line => (line.split(","))).map(line => (line(0), line(1)))
val combined = prcpRDD.join(violation).map(line => (line._2._1, line._2._2.toInt)).groupByKey
val sum = combined.map(line => (line._1.toFloat,line._2.toList.sum)).sortBy(_._1)
val prcpViolationDF = spark.createDataFrame(sum).toDF("PRCP","Count")
prcpViolationDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/prcpVSViolation.csv")

//mahattan data
val crimeFile = sc.textFile("/user/yl6127/project/crime/").map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
val ma = crimeFile.filter(line => line(4) == "MANHATTAN").map(line => (line(0),line(1),line(2),line(3),line(5),line(6)))
val mahattanDF = spark.createDataFrame(ma).toDF("date","hour","crime_type","crime_level","latitude","longitude")
val df = mahattanDF.sort("date")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").option("delimiter",";").save("/user/yl6127/project/analysis/manhattanCrime")


//snow vs general crime
val weatherFile = sc.textFile("/user/cy1505/proj/cleaned_weather_data/part-00000-d6448b40-b8cd-4184-a6e5-9e597bb304de-c000.csv").filter(line=>(!line.contains("AWND"))).map(line => line.replace(",,",",0,")).map(line => line.split(","))
val snowRDD = weatherFile.map(line => (line(0), line(3)))
val generalCrime = sc.textFile("/user/yl6127/project/analysis/crimeCountByDay/").map(line => (line.split(","))).map(line => (line(0), line(1)))
val combined = snowRDD.join(generalCrime).map(line => (line._2._1, line._2._2.toInt)).groupByKey
val sum = combined.map(line => (line._1.toFloat,line._2.toList.sum)).sortBy(_._1)
val snowCrimeDF = spark.createDataFrame(sum).toDF("Snow","Count")
snowCrimeDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/yl6127/project/analysis/snowVSCrime.csv")



// Profiling 311 Complaints regarding Drinking and Homeless Encampment with Crime data
/*
Transform each line in 311 data into a Tuple in this format: (month, date, year, hour, latitude, longitude, type of complaint)
*/
def myMap(L:Array[String])={
  val A = L(0).split("/")
  val mm = A(0).toInt
  val dd = A(1).toInt
  val yyyy = A(2).toInt
  val hour = L(1).toInt
  val longi = L(3).toDouble
  val lati = L(4).toDouble
  val cType:String = L(2)
  (mm,dd,yyyy,hour,lati,longi,cType)
}

/*
Transform each line in police arrest data into a Tuple in this format: (month, date, year, hour, latitude, longitude, type of crime, level of crime)
*/
def crimeMap(L:Array[String])={
  val A = L(0).split("/")
  val mm = A(0).toInt
  val dd = A(1).toInt
  val yyyy = A(2).toInt
  val hour = L(1).toInt
  val lati = L(5).toDouble
  val longi = L(6).toDouble
  val crimeType:String = L(2)
  val crimeLevel:String = L(3)
  (mm,dd,yyyy,hour,lati,longi,crimeType,crimeLevel)
}

// import and enable Spark sql context
import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import sqlCtx._

// load 311 complaint data, parse it, and convert to formatted tuple
val TOO = sc.textFile("/user/jl8456/BDAD/project/311_data_cleaned_manhattan")
val parsedTOO = TOO.map(L => L.split(","))

// add unique id for each 311 complaints. This is used for the purpose of finding DISTINCT record during sql.
val converted_with_id_TOO = parsedTOO.map(myMap).zipWithUniqueId

// flatten the id into the tuple along with other fields
val convertedTOO = converted_with_id_TOO.map(L=> (L._2,L._1._1,L._1._2,L._1._3,L._1._4,L._1._5,L._1._6,L._1._7))

// load crime data, parse it, convert to formatted tuple
val crimeRDD = sc.textFile("/user/yl6127/project/crime/").map(L=> (L.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)))

// transform the data, add id for each transformed record. This is used for the purpose of finding DISTINCT record during sql.
val converted_with_id_crime = crimeRDD.map(crimeMap).zipWithUniqueId
// flatten the id into the tuple along with other fields
val convertedCrime = converted_with_id_crime.map(L=> (L._2,L._1._1,L._1._2,L._1._3,L._1._4,L._1._5,L._1._6,L._1._7,L._1._8))

// register dataframes to sql table
val crimeDF = sqlCtx.createDataFrame(convertedCrime).toDF("cid","mm","dd","yyyy","hour","lat","lng","crimeType","crimeLevel")
val tooDF = sqlCtx.createDataFrame(convertedTOO).toDF("cid","mm","dd","yyyy","hour","lat","lng","type")

tooDF.registerTempTable("too")
crimeDF.registerTempTable("crime")

// unused UDF functions, used to be part of analysis
/*
val squared = (s: Long) => {
  s * s
}
val sqrt = (s: Double) => {
  Math.sqrt(s)
}
spark.udf.register("SQUARE", squared)
spark.udf.register("SQRT", sqrt)
*/

/*
Get the number of crimes in total
*/
sqlCtx.sql("""
SELECT COUNT(*)
FROM crime
""").collect
// There is a total of 4575060 recorded crimes

/*
Get the number of crimes happened near a 311 complaint regarding homeless encampment within a block radius.
Crimes considered must occur within 6 hours after the 311 complaint call.
*/
val homelessCrimeRDD = sqlCtx.sql("""
SELECT COUNT(DISTINCT C.cid)
FROM crime C, too T
WHERE C.yyyy = T.yyyy
AND T.type = "homeless encampment"
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 ) * 24  + T.hour)) <= 6
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 )  * 24  + T.hour)) >= 0
AND ABS(T.lat - C.lat) <= 0.0018 AND ABS(T.lng - C.lng) <= 0.0018
""")
homelessCrimeRDD.collect
// There is a total of 6222 crimes occurred near homeless encampment area


/*
Get the number of crimes happened near a 311 complaint regarding drinking in public within a block radius.
The crime must occur within 4 hours after the 311 complaint call.
*/
val drinkingCrimeRDD = sqlCtx.sql("""
SELECT COUNT(DISTINCT C.cid)
FROM crime C, too T
WHERE C.yyyy = T.yyyy
AND T.type = "drinking in public"
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 ) * 24  + T.hour)) <= 4
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 )  * 24  + T.hour)) >= 0
AND ABS(T.lat - C.lat) <= 0.0018 AND ABS(T.lng - C.lng) <= 0.0018
""").rdd.persist.map(row => row.mkString)

drinkingCrimeRDD.collect
// There is a total of 333 crimes occurred near complaints regarding drinking in public area

/*
Get the number of crimes happened near a 311 complaint regarding broken/flashy street light within a block radius.
The crime must occur within 2 weeks(14 days) after the 311 complaint call.
*/
val streetLightCrimeRDD = sqlCtx.sql("""
SELECT COUNT(DISTINCT C.cid)
FROM crime C, too T
WHERE (C.dd + C.mm*30 + (C.yyyy%2000)*365) - (T.dd+T.mm*30 + (T.yyyy%2000)*365) <= 14
AND T.type = "street light"
AND (C.dd + C.mm*30 + (C.yyyy%2000)*365) - (T.dd+T.mm*30 + (T.yyyy%2000)*365) >= 0
AND ABS(T.lat - C.lat) <= 0.0018 AND ABS(T.lng - C.lng) <= 0.0018
""").collect
// There is a total of 150804 crimes occurred near broken/flashy street light area

/*
Top 10 crime types occurs near 311 complaints regarding homeless encampment.
Crimes considered must occur within 6 hours after the complaint and within a block radius.
*/
// find out each distinct crime happened near distinct 311 complaint id regarding homeless encampment to avoid double counts
val homelessCrimeTypes = sqlCtx.sql("""
SELECT DISTINCT C.cid, T.cid, C.crimeType
FROM crime C, too T
WHERE C.yyyy = T.yyyy
AND T.type = "homeless encampment"
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 ) * 24  + T.hour)) <= 6
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 )  * 24  + T.hour)) >= 0
AND ABS(T.lat - C.lat) <= 0.0018 AND ABS(T.lng - C.lng) <= 0.0018
""").rdd.persist.map(row=>(row(2),1))
// group the crime type as a tuple, perform counting and sort by desc.
val h_c_types_top_ten = homelessCrimeTypes.reduceByKey(_ + _, 1).sortBy(_._2, ascending=false)

h_c_types_top_ten.take(10).foreach(println)
/*
(PETIT LARCENY,2283)                                                            
(GRAND LARCENY,1261)
(HARRASSMENT 2,706)
(CRIMINAL MISCHIEF & RELATED OF,390)
(ASSAULT 3 & RELATED OFFENSES,364)
(OFF. AGNST PUB ORD SENSBLTY &,291)
(BURGLARY,149)
(DANGEROUS DRUGS,149)
(FELONY ASSAULT,129)
(ROBBERY,111)
*/

/*
Top 10 crime types occurs near 311 complaints regarding drinking in public.
Crimes considered must occur within 6 hours after the complaint and within a block radius.
*/
// find out each distinct crime happened near distinct 311 complaint id regarding drinking in public to avoid double counts
val drinkingInPublicCrimeTypes = sqlCtx.sql("""
SELECT DISTINCT C.cid, T.cid, C.crimeType
FROM crime C, too T
WHERE C.yyyy = T.yyyy
AND T.type = "drinking in public"
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 ) * 24  + T.hour)) <= 4
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 )  * 24  + T.hour)) >= 0
AND ABS(T.lat - C.lat) <= 0.0018 AND ABS(T.lng - C.lng) <= 0.0018
""").rdd.persist.map(row=>(row(2),1))
// group the crime type as a tuple, perform counting and sort by desc.
val d_p_types_top_ten = drinkingInPublicCrimeTypes.reduceByKey(_ + _, 1).sortBy(_._2, ascending=false)
d_p_types_top_ten.take(10).foreach(println)
/*
(PETIT LARCENY,64)                                                              
(HARRASSMENT 2,49)
(GRAND LARCENY,43)
(ASSAULT 3 & RELATED OFFENSES,38)
(CRIMINAL MISCHIEF & RELATED OF,19)
(OFF. AGNST PUB ORD SENSBLTY &,18)
(FELONY ASSAULT,16)
(DANGEROUS DRUGS,16)
(ROBBERY,10)
(OFFENSES AGAINST PUBLIC ADMINI,8)
*/

/*
Top 10 crime types occurs near 311 complaints regarding broken/flashy street light.
The crime must occur within 2 weeks(14 days) after the 311 complaint call.
*/
// find out each distinct crime happened near distinct 311 complaint id regarding street light to avoid double counts
val streetLightCrimeTypes = sqlCtx.sql("""
SELECT DISTINCT C.cid, T.cid, C.crimeType
FROM crime C, too T
WHERE C.yyyy = T.yyyy
AND T.type = "street light"
AND (C.dd + C.mm*30 + (C.yyyy%2000)*365) - (T.dd+T.mm*30 + (T.yyyy%2000)*365) >= 0
AND ABS(T.lat - C.lat) <= 0.0018 AND ABS(T.lng - C.lng) <= 0.0018
""").rdd.persist.map(row=>(row(2),1))
// group the crime type as a tuple, perform counting and sort by desc.
val s_c_types_top_ten = streetLightCrimeTypes.reduceByKey(_ + _, 1).sortBy(_._2, ascending=false)
s_c_types_top_ten.take(10).foreach(println)
/*
(PETIT LARCENY,786248)                                                          
(GRAND LARCENY,442620)
(HARRASSMENT 2,289890)
(ASSAULT 3 & RELATED OFFENSES,242879)
(CRIMINAL MISCHIEF & RELATED OF,235982)
(OFF. AGNST PUB ORD SENSBLTY &,127367)
(DANGEROUS DRUGS,118027)
(BURGLARY,80813)
(FELONY ASSAULT,79059)
(ROBBERY,77499)
*/

/*
Save all base tables:
Save each type of 311 complaints to separate dataframes.
Dataframe dir names:
homeless
drinking
streetlight
Save police arrest data as a data frame. Dataframe dir name: crime

These data are saved for feature extraction using machine learning.

Location: /user/jl8456/BDAD/project/dataframes/[dataframe dir name]
Usage: substitute dataframe name to one of dataframe name above.
*/

// base table: homeless
sqlCtx.sql("""
SELECT *
FROM too T
WHERE T.type = "homeless encampment"
""").coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/jl8456/BDAD/project/dataframes/homeless")

// base table: drinking
sqlCtx.sql("""
SELECT *
FROM too T
WHERE T.type = "drinking in public"
""").coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/jl8456/BDAD/project/dataframes/drinking")

// base table: streetlight
sqlCtx.sql("""
SELECT *
FROM too T
WHERE T.type = "street light"
""").coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/jl8456/BDAD/project/dataframes/streetlight")

// base table: crime
crimeDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/user/jl8456/BDAD/project/dataframes/crime")

// ETL for feature extraction
import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import sqlCtx._

val crime = spark.read.format("csv").option("header","true").option("inferSchema","true").load("hdfs:///user/cy1505/proj/crime.csv")
val homeless = spark.read.format("csv").option("header","true").option("inferSchema","true").load("hdfs:///user/cy1505/proj/homeless.csv")
val drinking = spark.read.format("csv").option("header","true").option("inferSchema","true").load("hdfs:///user/cy1505/proj/drinking.csv")
val streetlight = spark.read.format("csv").option("header","true").option("inferSchema","true").load("hdfs:///user/cy1505/proj/streetlight.csv")

crime.registerTempTable("crime")
homeless.registerTempTable("homeless")
drinking.registerTempTable("drinking")
streetlight.registerTempTable("streetlight")

sqlCtx.sql("""
SELECT DISTINCT C.cid
FROM crime C, homeless T
WHERE C.yyyy = T.yyyy
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 ) * 24  + T.hour)) <= 6
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 )  * 24  + T.hour)) >= 0
AND ABS(T.lat - C.lat) <= 0.0018 AND ABS(T.lng - C.lng) <= 0.0018
""")

sqlCtx.sql("""
SELECT DISTINCT C.cid
FROM crime C, drinking T
WHERE C.yyyy = T.yyyy
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 ) * 24  + T.hour)) <= 4
AND (( C.dd+C.mm*30 ) * 24 + C.hour - (( T.dd+T.mm*30 )  * 24  + T.hour)) >= 0
AND ABS(T.lat - C.lat) <= 0.0018 AND ABS(T.lng - C.lng) <= 0.0018
""")

sqlCtx.sql("""
SELECT DISTINCT C.cid
FROM crime C, streetlight T
WHERE C.yyyy = T.yyyy
AND (C.dd + C.mm*30)- (T.dd+T.mm*30) <= 14
AND (C.dd + C.mm*30) - (T.dd+T.mm*30) >= 0
AND ABS(T.lat - C.lat) <= 0.0018 AND ABS(T.lng - C.lng) <= 0.0018
""")

sqlCtx.sql("""
SELECT DISTINCT C.cid
FROM crime C, streetlight T
WHERE (C.dd + C.mm*30 + (C.yyyy%2000)*365) - (T.dd+T.mm*30 + (T.yyyy%2000)*365) <= 14
AND (C.dd + C.mm*30 + (C.yyyy%2000)*365) - (T.dd+T.mm*30 + (T.yyyy%2000)*365) >= 0
AND ABS(T.lat - C.lat) <= 0.0018 AND ABS(T.lng - C.lng) <= 0.0018
""")

coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("hdfs:///user/cy1505/proj/cidwithhomeless.csv")



