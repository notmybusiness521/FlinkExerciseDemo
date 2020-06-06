import org.apache.flink.streaming.api.scala._

object FlinkTransform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //1Map操作
//    val stream = env.generateSequence(1, 10)
//    val streamMap = stream.map(x => math.pow(x, 2))
    //2flatMap操作
//    val stream = env.readTextFile("E:\\data\\flinktest\\test0.txt")
//    val streamFlatMap = stream.flatMap(line => line.split(" "))
//    streamFlatMap.print()
    //3filter
//    val streamFilter = stream.filter(line => line.contains("hadoop"))
//    streamFilter.print()
    //4connect
    val stream0 = env.readTextFile("E:\\data\\flinktest\\test0.txt").flatMap(line=>line.split(" "))
    val stream1 = env.fromCollection(List(1,2,3,4,5))
    val streamConnect = stream0.connect(stream1)
    streamConnect.map(item => (item, 1), x=> x*2).print()
    env.execute("TranformDemo")

  }

}
