/*
Jiaqi Liu
BDAD
Project Data ETL
*/

/*
This function filters out complaint types that are irrelevant to the topic of this analytic.
Only 311 complaints related to the following three aspects will remain:
1. street light conditions
2. drinking in public
3. homeless encampment
*/
def topicFilter(L:String):Boolean = {
  val lowerStr:String = L.toLowerCase
  (lowerStr.contains("street light") ||
  (lowerStr.contains("drinking") && lowerStr.contains("in public")) ||
  lowerStr.contains("homeless encampment")) && lowerStr.contains("manhattan")
}

/*
This function filters out data that does not contain time or location information
*/
def dateTimeFilter(L:String):Boolean = {
  val A = L.split(",")
  val date = A(1)
  //val LatStr = A(A.length-4)
  //val LongStr = A(A.length-3)
  date != "" && A(A.length-2).contains("(")
}

/*
This function transforms filtered data by
1. drop irrelevant data fields - eg.
2. convert remaining data fields according to the schema
*/
def transformData(L:String):String = {
  val line = L.toLowerCase
  val A = L.split(",")

  // extract date
  val dateArr = A(1).split(" ")
  val Date = dateArr(0)

  // extract hour of the day and convert them to 24 hour standard
  val TimeArr = dateArr(1).split(":")
  var Hour:Int = TimeArr(0).toInt
  if(A(A.length-1).toUpperCase == "PM" && Hour != 12){
    Hour += 12
  }
  else if(A(A.length-1).toUpperCase == "AM" && Hour == 12){
    Hour = 0
  }

  // extract location
  val Latitude:Double = A(A.length-3).toDouble
  val Longitude:Double = A(A.length-4).toDouble

  // extract complaint type
  var Type:String = null
  if(line.contains("drinking")){
    Type = "drinking in public"
  }
  else if(line.contains("street light")){
    Type = "street light"
  }
  else{
    Type = "homeless encampment"
  }

  // convert all transformed data to a tuple
  val finalTuple = (Date,Hour,Type,Latitude,Longitude)
  val n = finalTuple.toString.length

  // return value based on the tuple
  finalTuple.toString.substring(1,n-1)
}

// load data from DUMBO HDFS
val TOO = sc.textFile("/user/jl8456/BDAD/project/311_data.csv")

// filter out complaints types that are not relevant to our topic
val complaintsRDD = TOO.filter(topicFilter(_))

// filter our complaints that does not have a recorded date and a location
val filteredRDD = complaintsRDD.filter(dateTimeFilter)

// drop unrelated fields and transform data according to the schema
val finalRDD = filteredRDD.map(transformData(_))

// store the cleaned data to HDFS
finalRDD.saveAsTextFile("/user/jl8456/BDAD/project/311_data_cleaned_manhattan")
