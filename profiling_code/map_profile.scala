// load the parquet file from HDFS as dataframe
val street = spark.read.option("header", "true").load("hdfs:///user/cy1505/proj/etl_manhattan_map.parquet"

// show the sample contents in the dataframe
street.show
/*
+----------+--------------+-------------+--------------------+
|PHYSICALID|    FULL_STREE|   SHAPE_Leng|            polygons|
+----------+--------------+-------------+--------------------+
|    170875|      DRIVEWAY|675.330704056|[[40.859518504454...|
|    183608|   BENNETT AVE|735.721079105|[[40.850447325205...|
|       488|      STONE ST|122.538700052|[[40.704545914131...|
|     79723|       GOLD ST|159.149458558|[[40.707857835737...|
|    179568|     E  109 ST|508.092565157|[[40.796216984878...|
|      3904| AMSTERDAM AVE|38.0698690576|[[40.846710523612...|
|      1121|    EIGHTH AVE|257.894368484|[[40.749052196220...|
|     21835|      E  61 ST|714.075355588|[[40.760858092625...|
|       474|  GREENWICH ST|262.936961672|[[40.735722923714...|
|    176643|F ROOSEVELT DR| 155.33004268|[[40.708455379220...|
|      3667|         3 AVE|286.455576401|[[40.769064407802...|
|     72676| HARLEM RIV DR|74.7515848965|[[40.814055471812...|
|     25124|     W  104 ST|47.1168177558|[[40.800967502394...|
|      2347|      PARK AVE|264.701963371|[[40.773097704098...|
|      1335|   SEVENTH AVE|244.100971221|[[40.740469821557...|
|     75924|BRONX SHORE RD|41.2556067107|[[40.800027111436...|
|      1548|F ROOSEVELT DR|14.7872235516|[[40.758640232110...|
|      2289|  COLUMBUS AVE|149.385237812|[[40.772618029843...|
|     16526|    FOURTH AVE|262.563940176|[[40.731288523324...|
|    132675| HARLEM RIV DR|385.493252055|[[40.802945281045...|
+----------+--------------+-------------+--------------------+
only showing top 20 rows
*/

// register temp view for the dataframe
street.createOrReplaceTempView("street")

// check the file schema
street.printSchema
/*
root
 |-- PHYSICALID: integer (nullable = true)
 |-- FULL_STREE: string (nullable = true)
 |-- SHAPE_Leng: double (nullable = true)
 |-- polygons: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- _1: double (nullable = true)
 |    |    |-- _2: double (nullable = true)
*/

// check the total amount of street records
spark.sql("SELECT COUNT(*) FROM street S").show
/*
+--------+
|count(1)|
+--------+
|   11851|
+--------+
*/


// profile the physicalid column
spark.sql("SELECT MIN(S.PHYSICALID) FROM street S").show
/*
+---------------+
|min(PHYSICALID)|
+---------------+
|              3|
+---------------+
*/

spark.sql("SELECT MAX(S.PHYSICALID) FROM street S").show

/*
+---------------+
|max(PHYSICALID)|
+---------------+
|         187020|
+---------------+
*/

// output the longest street name string
spark.sql("SELECT DISTINCT FULL_STREE FROM street WHERE LENGTH(FULL_STREE) = (SELECT MAX(LENGTH(FULL_STREE)) FROM street)").take(1).foreach(println)
// [MACOMBS DAM BRIDGE PEDESTRIAN PATH]

// output the shortest street name string
spark.sql("SELECT DISTINCT FULL_STREE FROM street WHERE LENGTH(FULL_STREE) = (SELECT MIN(LENGTH(FULL_STREE)) FROM street)").take(1).foreach(println)
// [D RD]

// extract the range of SHAPE_Leng column
spark.sql("SELECT MIN(S.SHAPE_Leng) FROM street S").show
/*
+---------------+
|min(SHAPE_Leng)|
+---------------+
|  4.51125618723|
+---------------+
*/

spark.sql("SELECT MAX(S.SHAPE_Leng) FROM street S").show
/*
+---------------+
|max(SHAPE_Leng)|
+---------------+
|  6974.75117964|
+---------------+
*/

// extract largest and smallest polygons coordinates
spark.sql("SELECT MAX(S.polygons) FROM street S").take(1).foreach(println)
/*
[WrappedArray([40.878481129756736,-73.92535763462081], [40.87888836785315,-73.92490327473082])]
*/

spark.sql("SELECT MIN(S.polygons) FROM street S").take(1).foreach(println)
/*
[WrappedArray([40.684791450875544,-74.0219117257278], [40.684209358950184,-74.02319693657114], [40.6841423988586,-74.0234094754425], [40.68409131197947,-74.02363328038228], [40.6840575577275,-74.02386521806206], [40.68404210091585,-74.02410175141092], [40.684045328800416,-74.02433910858124], [40.68406704449714,-74.02457352676119], [40.68410647365904,-74.0248013628507], [40.684162321541734,-74.02501936282704], [40.68419093772354,-74.0251892598213], [40.68423488267078,-74.02535653517499], [40.68429388931087,-74.02551761410291], [40.684367048759924,-74.02566906679617], [40.68445286589649,-74.0258077961492], [40.68454936568734,-74.02593125588132], [40.68465420136511,-74.02603762223173])]
*/
