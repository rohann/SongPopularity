%spark
//PARSING
val csvTab = "com.databricks.spark.csv"
val csvOptionsTab = Map("mode" -> "DROPMALFORMED", "delimiter" -> "\t", "header" -> "true")
val songA = sqlContext.read.format(csvTab).options(csvOptionsTab).load("/rohan-experiments/songOutputAHeader.txt")
val songB = sqlContext.read.format(csvTab).options(csvOptionsTab).load("/rohan-experiments/songOutputBHeader.txt")
val song = songA.unionAll(songB)
song.show

//PART 1
import org.apache.spark.sql.Row
val songVartist = song.select("artist_hotttnesss","hot").filter("hot>0")

val songVartistRDD = songVartist.map(record=>{
    val row1 = BigDecimal(record(0).toString.toDouble).setScale(1, BigDecimal.RoundingMode.HALF_UP)
    val row2 =  BigDecimal(record(1).toString.toDouble).setScale(1, BigDecimal.RoundingMode.HALF_UP)
    (row1.toDouble,row2.toDouble)
})

val topSongs = song.filter("hot>=0").select("hot")
topSongs.registerTempTable("topSongs")
val sorted = topSongs.sort(desc("hot"))

val t = (Math.round(sorted.count * 0.30)).asInstanceOf[Int]
val sumArray = sorted.take(t)

val topArtists = song.filter("hot>=0.63").select("artist_hotttnesss")
topArtists.count

val topArtistsRDD = topArtists.map(record=>{
    val row1 = BigDecimal(record(0).toString.toDouble).setScale(1, BigDecimal.RoundingMode.HALF_UP)
    (row1.toDouble)
})

val topArtistsMap = topArtistsRDD.map(row=>(row,1)).reduceByKey(_+_)
case class topArtistsClass(artists_hottness:Double,count:Integer)
val topArtistsDF = topArtistsMap.map{
    case (s0,s1) => topArtistsClass(s0,s1)
}.toDF
topArtistsDF.registerTempTable("topArtists")

%sql
SELECT * FROM topArtists ORDER BY artists_hottness

//PART 2


%spark
// getting correlation between loudness and song_hotttnesss
val loudVHot = song.filter("hot>=0").select("loudness","hot")
val loudVHotRDD = loudVHot.map(record=>{
    val row2 = BigDecimal(record(1).toString.toDouble).setScale(1, BigDecimal.RoundingMode.HALF_UP)
    (record(0).toString.toDouble,row2.toDouble)
})
case class loudVHotClass(loud:Double,hot: Double)




val loudVHotDF = loudVHotRDD.map{
    case (s0,s1) => loudVHotClass(s0,s1)
}.toDF
loudVHotDF.registerTempTable("loudVHot")


%sql
SELECT * FROM loudVHot  ORDER BY hot ASC


//getting correlation between duration and song_hotttness
val durationVHot = song.filter("hot>=0").select("duration","hot")


val durationVHotRDD = durationVHot.map(record=>{
    val row1 = BigDecimal(record(0).toString.toDouble).setScale(0, BigDecimal.RoundingMode.HALF_UP)  
    val row2 = BigDecimal(record(1).toString.toDouble).setScale(1, BigDecimal.RoundingMode.HALF_UP)
    (row1.toDouble,row2.toDouble)
})
case class DurationVHot(duration:Double,hot: Double)


val durationVHotDF = durationVHotRDD.map{
    case (s0,s1) => DurationVHot(s0,s1)
}.toDF
durationVHotDF.registerTempTable("durationVHot")


%sql
SELECT * FROM durationVHot ORDER BY hot ASC

