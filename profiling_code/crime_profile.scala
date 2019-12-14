// profiling

val field1 = rdd4.map(line => line._1).filter(field => !field.contains("null")).filter(line => (line.length() - line.replaceAll("/","").length() == 2))
val field1_year = field1.map(line => line.split("/")(2).toInt)
field1_year.min
// 2010
field1_year.max
// res3: Array[String] = Array(2018)

val field1_month_2019 = field1.filter(line => (line.split("/")(2).toInt==2019)).map(line => line.split("/")(0).toInt)
field1_month_2019.max
// 6

val field1_date_2019 = field1.filter(line => (line.split("/")(2).toInt==2019)).filter(line => (line.split("/")(0).toInt==6)).map(line => line.split("/")(1).toInt)
field1_date_2019.max
// 30

val field2 = rdd4.map(line => line._2).filter(field => !field.contains("null")).map(line => line.toInt)
field2.min()
field2.max()


val field3 = rdd4.map(line => line._3).filter(field => !field.contains("null"))
field3.distinct().count()
field3.distinct().saveAsTextFile("/user/yl6127/project/schema/")

val field4 = rdd4.map(line => line._4)
field4.distinct().foreach(println)
// res8: Array[String] = Array(MISDEMEANOR, VIOLATION, FELONY)

val field5 = rdd4.map(line => line._5).filter(field => !field.contains("null"))
field5.distinct().foreach(println)
// res0: Array[String] = Array(MANHATTAN, BROOKLYN, STATEN ISLAND, QUEENS, BRONX)

val field6 = rdd4.map(line => line._6).filter(field => !field.contains("null")).map(line => line.toDouble)
field6.max
// 59.657273946

//after cleaning
field6.max
// 40.915047885

field6.min
// 40.498905363

val field7 = rdd4.map(line => line._7).filter(field => !field.contains("null")).map(line => line.toDouble)
field7.max
//res6: Double = -73.684788384
field7.min
//res5: Double = -77.519206334