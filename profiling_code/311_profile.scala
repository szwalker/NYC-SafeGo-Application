// load 311 complaint data
val TOO = sc.textFile("/user/jl8456/BDAD/project/311_data_cleaned_manhattan")

// show the data format
TOO.take(5).foreach(println)
/*
11/06/2013,2,homeless encampment,-73.9810241610079,40.74607379727146
11/06/2013,10,homeless encampment,-73.9890842202348,40.76027341650346
11/06/2013,2,homeless encampment,-73.98033946526458,40.73044231804128
11/06/2013,2,homeless encampment,-73.98033946526458,40.73044231804128
11/06/2013,10,homeless encampment,-73.97969730467524,40.730175965340905
*/

val parsedTOO = TOO.map(L => L.split(","))

// get the range of month column
parsedTOO.map(L=>L(1).toInt).max
// 12
parsedTOO.map(L=>L(1).toInt).min
// 1

// get the range of the complaint type column, check all possible values
parsedTOO.map(L=>L(2)).distinct.collect.foreach(println)
/*
drinking in public
homeless encampment
street light
*/

// get the range of longitudes of 311 complaint's area
parsedTOO.map(L=>L(3).toDouble).min
// Double = -74.24345032197097
parsedTOO.map(L=>L(3).toDouble).max
// Double = -73.8565722971044

// get the range of latitude of 311 complaint's area
parsedTOO.map(L=>L(4).toDouble).min
// Double = 40.49965901258716
parsedTOO.map(L=>L(4).toDouble).max
// Double = 40.89971478361068

// get the range of 311 complaint month
parsedTOO.map(L=>L(0).split("/")(0).toInt).min
// 1
parsedTOO.map(L=>L(0).split("/")(0).toInt).max
// 12

// get the range of 311 complaint day
parsedTOO.map(L=>L(0).split("/")(1).toInt).min
// 1
parsedTOO.map(L=>L(0).split("/")(1).toInt).max
// 31

// get the range of 311 complaint year
parsedTOO.map(L=>L(0).split("/")(2).toInt).min
// 2010
parsedTOO.map(L=>L(0).split("/")(2).toInt).max
// 2019
