package the.sum

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1 创建流处理环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2 读取套接字文件流
    val value = environment.socketTextStream("hadoop102", 7777)

    // 3
    val value1 = value.flatMap(_.split(" ")).map(word => (word, 1))

    // 4
    val value2 = value1.keyBy(_._1)

    // 5
    val value3 = value2.sum(1)

    // 6
    value3.print()

    // 7 执行环境
    environment.execute()
  }
}
