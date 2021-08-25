import java.util
import java.util.Properties

object getProperties {
  val props = getProperties()
  val filePath = props.getProperty("spark.InvertedIndexFilePath")

  def getProperties(): Properties ={
    val props = new Properties()
    val in=this.getClass.getClassLoader.getResourceAsStream("geektime_spark.properties")
    props.load(in)
    props
  }
}
