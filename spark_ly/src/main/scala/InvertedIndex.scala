import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object InvertedIndex {
  def main(args:Array[String]):Unit={
      val spark = SparkSession.builder().master("local[1]")
        .appName("InvertedIndexTest").getOrCreate()
      val sc: SparkContext = spark.sparkContext
      sc.setLogLevel("ERROR")
       val fileRdd = sc.textFile(getProperties.filePath).flatMap({
         file =>
                val fileArray = file.split('.')
                val fileName = fileArray(0)
               fileArray(1).split(" ").filter(_!="")
                 .map(index=>(fileName,index)).distinct
       })
      fileRdd.map(kv=>(kv._2+":",kv._1)).reduceByKey(_+","+_)
      .collect().foreach(r=>println(r))

      spark.stop()
  }
}


