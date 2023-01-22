package the.sum

import org.apache.flink.streaming.api.scala._

object BoundedStreamWordCount {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val value = environment.readTextFile("input/data.txt")

    val value1: DataStream[(String, Int)] = value.flatMap(_.split(" ")).map(word => (word, 1))

    val value2: KeyedStream[(String, Int), String] = value1.keyBy(_._1)

    val value3: DataStream[(String, Int)] = value2.sum(1)

    value3.print()

    environment.execute()
  }
}
