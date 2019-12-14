// etl maps
val centerline = spark.read.format("csv").option("header","true").option("inferSchema","true").load("hdfs:///user/cy1505/proj/Centerline.csv")
val maps = centerline.select($"PHYSICALID",$"the_geom", $"BOROCODE",$"FULL_STREE",$"RW_TYPE", $"SHAPE_Leng")
val manhattan_maps = maps.filter($"BOROCODE" === 1).drop($"BOROCODE")
val valid_maps = manhattan_maps.filter($"RW_TYPE".isin(1,2,3,4,5,8,9,10,13)).drop("RW_TYPE")
def etlGeo(geo:String) = geo.stripPrefix("MULTILINESTRING ((").stripSuffix("))").split(",").map(x => { val y = x.stripPrefix(" ").stripSuffix(" ").split(" "); (y(0).toDouble,y(1).toDouble);})


val udfEtlGeo = udf(geo:String => geo.stripPrefix("MULTILINESTRING ((").stripSuffix("))").split(",").map(x => { val y = x.stripPrefix(" ").stripSuffix(" ").split(" "); (y(1).toDouble,y(1).toDouble) }));

val etled_maps = valid_maps.withColumn("polygons", udfEtlGeo($"the_geom")).drop("the_geom")

etled_maps.write.format("parquet").option("header", "true").save("hdfs:///user/cy1505/proj/etl_manhattan_map.parquet")


// read from map
val nyc_map = spark.read.option("header", "true").load("hdfs:///user/cy1505/proj/etl_manhattan_map.parquet"
