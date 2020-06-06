import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._



object FlinkWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val path = new Path("E:\\data\\flinktest\\test0.txt")
//    val stream = {
//      env.readFile(new TextInputFormat(path), "E:\\data\\flinktest\\test0.txt")
//    }
//    val stream = env.socketTextStream("localhost", 11111)
    val stream = env.readTextFile("E:\\data\\flinktest\\test1.txt")
    val list = List(1,2,3,4)
    val iter = Iterator(12,2,3,4,5)
//    val stream = env.fromCollection(list)
//    val stream = env.generateSequence(1, 10)
    val wc = stream.flatMap(line => line.split(" ")).map(word=>(word, 1)).keyBy(0).sum(1)
    wc.print()
    env.execute("FirstJob")
  }
}
