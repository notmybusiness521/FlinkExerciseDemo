import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import scala.collection.mutable

object TimeWindowDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("localhost", 1111)

    val middleRes = stream.map(word => (word, 1))
      .map(ele => {
        ("{" + "\"k\":" + "\"" + ele._1 + "\"" +"," + "\"v\":" + "\"" + ele._2 + "\"" + "}")
      })
    val window = middleRes.timeWindowAll(Time.seconds(3))
      .reduce(_ + "@" + _)
      .map(e => {
        val hashSet = new mutable.HashSet[String]()
        val windowData = e.split("@")
        for (item<- windowData){
          hashSet.add(item)
        }
        hashSet
      }).flatMap(r => r)
    window.print()
    env.execute("Test")
  }

}
