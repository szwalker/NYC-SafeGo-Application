// read from csv
val data = spark.read.format("csv").option("header","true").option("inferSchema","True").load("hdfs:///user/cy1505/proj/cleaned_weather_data");

// show five record to inspect data
data.show(5);

// number of weather records
data.count
// -> 3468

// number of normal days
data.filter($"IS_NORMAL" === 1).count
// -> 1967

// average wind of the whole dataset
data.select(avg($"AWND")).show
// 2.407935542718147

// average rain drop of the whole dataset 
data.select(avg($"PRCP")).show
// 3.5316897347174017

// average min and max temperature of new york in the lastest ten years
data.select(avg($"TMIN")).show
// 9.403662053056523
data.select(avg($"TMAX")).show
// 17.289792387543258

// coldest day of new york
data.select(min($"TMIN")).show
// -> -18.2

// hottest day of new york
data.select(max($"TMAX")).show
// 40.0

// number of snowy day
data.filter($"IS_SNOW" === 1).count
// 88

