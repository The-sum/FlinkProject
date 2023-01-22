package the.sum

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet, createTypeInformation}

object FlinkDataSet01 {
  def main(args: Array[String]): Unit = {
    // 1 构建运行环境
    val environment = ExecutionEnvironment.getExecutionEnvironment

    // 2 读取文件内容
    val value = environment.readTextFile("input/data.txt")

    // 3 对数据进行扁平化操作，并转换存储格式
    val value1: DataSet[(String, Int)] = value.flatMap(_.split(" ")).map(word => (word, 1))

    // 4 对数据按照单词进行分组
    val value2: GroupedDataSet[(String, Int)] = value1.groupBy(0)

    // 5 按照单词进行聚合
    val value3: AggregateDataSet[(String, Int)] = value2.sum(1)

    // 6 打印输出
    value3.print()
  }
}
