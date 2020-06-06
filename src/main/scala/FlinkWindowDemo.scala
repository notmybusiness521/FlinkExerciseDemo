
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkWindowDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val stream = env.socketTextStream("localhost", 11111)
    val streamKeyBy = stream.map(item=>(item, 1)).keyBy(0)

//    val streamWindow = streamKeyBy.countWindow(5).reduce(
//      (it1, it2) => (it1._1, it1._2 + it2._2)
//    )
//    val streamWindow = streamKeyBy.countWindow(5, 2).reduce(
//      (it1, it2) => (it1._1, it1._2 + it2._2)
//    )
    val streamTimeWindow = streamKeyBy.timeWindow(Time.seconds(5),Time.seconds(2)).reduce(
            (it1, it2) => (it1._1, it1._2 + it2._2)
          )
    streamTimeWindow.print()
    env.execute("Window")
  }
}
