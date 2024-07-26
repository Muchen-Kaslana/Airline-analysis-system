package scala

import org.apache.spark.{SparkConf, SparkContext}


object Wash {
  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("清洗"))
    // 读取数据文件
    val lineRDD = sc.textFile("D:/yuanshi_flight.txt")
    // 将每行数据按制表符分割成数组
    val arrRDD = lineRDD.map(_.split("\t"))

    // 删除带有空字段的记录
    val nonEmptyRDD = arrRDD.filter(arr => !arr.contains(""))

    // 获取准点率字段的索引（准点率是最后一个字段
    val onTimeRateIndex = arrRDD.first().length - 1

    // 删除准点率小于0.75的记录
    val filteredRDD = nonEmptyRDD.filter(arr => arr(onTimeRateIndex).toDouble >= 0.75)

    // 将清洗后的数据保存到新文件
    filteredRDD.map(arr => arr.mkString("\t")).saveAsTextFile("D:/cleaned_data_test.txt")
  }
}
