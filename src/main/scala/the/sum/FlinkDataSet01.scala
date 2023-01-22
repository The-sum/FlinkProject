package the.sum

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet, createTypeInformation}

object FlinkDataSet01 {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment

    val value = environment.readTextFile("input/data.txt")

    val value1: DataSet[(String, Int)] = value.flatMap(_.split(" ")).map(word => (word, 1))

    val value2: GroupedDataSet[(String, Int)] = value1.groupBy(0)

    val value3: AggregateDataSet[(String, Int)] = value2.sum(1)

    value3.print()
  }
}
