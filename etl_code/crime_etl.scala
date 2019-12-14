val rdd1 = sc.textFile("/user/yl6127/project/NYPD_Complaint_Data_Historic.csv").map(line => line.replace(",,",",null,")).map(line => line.replace(",,",",null,")).map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))

val current1 = sc.textFile("/user/yl6127/project/NYPD_Complaint_Data_Current.csv").map(line => line.replace(",,",",null,")).map(line => line.replace(",,",",null,")).map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))

val current2 = current1.filter(line => !line.contains("CMPLNT_NUM")).map(line => (line(3),line(4),line(15),line(13),line(2),line(32),line(33)))

val current3 = current2.filter(line => (line._1.length() - line._1.replaceAll("/","").length() == 2)).filter(line => line._1.split("/")(2).toInt == 2019)

val rdd2 = rdd1.filter(line => !line.contains("CMPLNT_NUM")).map(line => (line(1),line(2),line(8),line(12),line(13),line(27),line(28)))

val rdd3 = rdd2.filter(line => (line._1.length() - line._1.replaceAll("/","").length() == 2)).filter(line => line._1.split("/")(2).toInt > 2009)

val unioned = current3.union(rdd3)

//make sure the latitude is within the boundary from the profiling code
val rdd4 = unioned.filter(line => (line._2.length() - line._2.replaceAll(":","").length() == 2)).filter(line => (line._6!="null")).filter(line => (line._6.toDouble < 41.0)).map(line => (line._1,line._2.split(":")(0),line._3,line._4,line._5,line._6,line._7))

val rdd5 = rdd4.map(line => line.toString()).map(line => line.substring(1,line.length-1)).filter(line => !line.contains("null"))

rdd5.saveAsTextFile("/user/yl6127/project/crime/")







