# Readme
### Article and Slides For Big Data Analytic Applications Symposium - Fall 2019
[Clic me to read the NYC SafeGo Application Article](https://github.com/szwalker/NYC-SafeGo-Application/blob/master/NYC%20SafeGo%20Application%20Article.pdf)
[Click me for presentation slide.](https://drive.google.com/file/d/1cBJGlKK_FXjgty0cGmv_EPr4-gH6nNOZ/view?usp=sharing)


### Datasets:
| NAME | RAW DATA LOCATION ON DUMBO |
| ------ | --------- |
| crime   | `hdfs:///user/yl6127/project/NYPD_Complaint_Data_Current.csv` `hdfs:///user/yl6127/project/NYPD_Complaint_Data_Historic.csv` |
| 311 | `hdfs:///user/jl8456/BDAD/project/311_data.csv` |
| Weather | `hdfs:///user/cy1505/proj/noaa_nyc_weather_2010-2019.csv`|
| NYC Street Centerline (CSCL)| `hdfs:///user/cy1505/proj/Centerline.csv`|


### Code Directory and Files:

```
├── data ingest              
│   └── crime_data_Ingest.txt
│   └── 311_data_Ingest.txt
│   └── weather_data_Ingest.txt
│   └── map_data_Injest.txt
|
├── profiling_code
│   ├── 311
│   │   ├── 311_profile.scala
│   ├── NYPD_Complaint
│   │   ├── crime_profile.scala
│   ├── weather
│   │   ├── weather_profile.scala
│   ├── map
│   │   ├── map_profile.scala
│   │  
├── etl_code
│   ├── 311
│   │   ├── 311_etl.scala
│   ├── NYPD_Complaint
│   │   ├── crime_etl.scala
│   ├── weather
│   │   ├── weather_etl.scala
│   ├── map
│   │   ├── map_etl.scala
│   │   
├── app_code
│   ├── crime_analysis.scala
│   ├── machine_learning
│   ├── backend
│   ├── frontend
├── screenshots
└── Readme
```
### Code Directory and File Description:

| NAME | DESCRIPTION |
| ------ | --------- |
| data ingest  | Upload data from local file system to Dumbo Cluster, and then ingest to HDFS |
| profiling_code | Profile the data using Spark-shell |
| etl_code | Clean the data using Spark-shell |
| code_iteration  | Conduct data analytics using Spark |

### Build and Run the Code:

+ **data ingest**:  
     Utilize ```scp``` command to upload the datasets from local file system to Dumbo Cluster. In Dumbo, using ```hdfs dfs -put file /user/yourNetID/project``` to ingest the data into Hadoop HDFS.

+ **profiling_code**:
     Utilize Spark to profile three datasets.
1. Compile and run code for 311 data:
  * spark2-shell -i crime_profile.scala
2. Compile and run code for NYPD Complaint data:
  * spark2-shell -i 311_profile.scala
3. Compile and run code for Weather data:
    * `spark2-shell -i weather_profile.scala`
4. Compile and run code for Map data:
    * `spark2-shell -i street(centerline)_profiling.scala`

+ **etl_code**:
     Utilize Spark to clean three datasets.
1. Compile and run code for 311 data:
  * `spark2-shell -i 311_etl.scala`
2. Compile and run code for NYPD Complaint data:
  * `spark2-shell -i crime_etl.scala`
3. Compile and run code for Weather data:
    * `spark2-shell -i weather_etl.scala`
4. Compile and run code for Map data:
    * `spark2-shell -i map_etl.scala`

+ **app_code**:
  1. code_analysis.scala:
  	* spark2-shell -i code_analysis.scala
  2. backend

* The backend of the server is implemented using **Apache Flask**.

* `app.py` - Our application (backend) that implements Safe Route API.

* API inputs:

  * 1. start point latitude

  * 2. start point longitude

  * 3. end point latitude

  * 4. end point longitude

* API output:

  * A json object with two fields that describes the safest route and the shortest route between start point and end point. One field is named `safest` and another one is named `shortest`.
  Each field is an array of street objects (please see more about the street object below.)
  ```
  {
    "safest":[...], ...
    "shortest":[...], ...
  }
  ```

* Both routes are implemented using Dijkstra's SSSP (Single Source Shortest Path) Algorithm.

* The street object returned by the api contains the following fields:

  * `sid`: Each street has a unique number id, this field is unique.

  * `streetLen`: a number denoting the physical street length of each street

  * `crimeIndex`: a number denoting the safety extent of a street

  * `Polygons`: an array of latitudes and longitudes numbers that represents the vertex of the street.


* How to run this application:

  * We have deploy this application online. Simply visit the deployed website of this application.

  * Or, if you prefer only to run the backend locally, you may deploy the backend code using Apache Flask use the following command: ```
  python3 app.py
  ```

* How to extract the results:

  * The backend api will take `get` requests from the following path: `/index?startLat=[a]&startLng=[b]&endLat=[c]&endLng=[d]`

  * Please replace `[a]` to start point latitude, `[b]` to start point longitude, `[c]` to end point latitude, and `[d]` to end point latitude.

  * Once the parameters are inputted correctly, the user will receive a 200 status code and the result will be send back in the json form mentioned above. Otherwise there will be a 400 status code and a return value of `Bad Request`.

3. frontend
  * The webpage frontend of the overall application.

  * `src/components/GoogleMapLoader.vue`:

  * This is a component that uses the google map API

  * `src/components/Search.vue`:

  * Search component that enable users to type in start point and destination

  * `src/components/Route.vue`:

  * Display the route sent back by backend

  * If you want to run the frontend locally and separately, use the following command:
  ```
    npm install
    npm run serve
  ```
    * If you want to run the frontend districbutedly, use the following command:
  ```
    npm install
    npm run build
  ```

4. machine-learning
  * The machine learning portion is implemented using **Spark MLLib**. The model is trained on DUMBO, and then downloaded and integrated into the application.
  * To run the machine learning inference independently, please use the following command (on dumbo)
    * Under the root, use `sbt package` to get `*.jar` files
    * To train the crime model
    ```
    spark2-shell -i train.scala
    ```
    * If run inference in shell, use
    ```
    spark2-shell -i inference.scala
    ```
    * If run inference as spark job, use
    ```
    spark2-submit --class inference --master yarn --deploy-mode client infernce.jar
    ```

### Team Works:
  * Yue Luo: 1/3 analytics + website frontend
  * Jiaqi Liu: 1/3 analytics + backend
  * Cong Yu: 1/3 analytics + Machine Learning

### References:
  * https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
  * https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html

#### Thank you for reading this file.
