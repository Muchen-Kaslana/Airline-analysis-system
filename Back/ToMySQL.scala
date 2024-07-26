package scala

// 导入必要的Spark和Scala库
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Properties

object ToMySQL {
    def main(args: Array[String]): Unit = {
      // 初始化Spark上下文，设置运行模式为本地模式，并指定应用名称
      val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("ToMySQL"))
      // 初始化SQL上下文，用于后续的数据处理
      val sqlContext = new SQLContext(sc)
      // 从文件系统中读取文本文件，并创建一个RDD[String]
      val lineRDD: RDD[String] = sc.textFile("D:/cleaned_data_test.txt")

      // 将每行数据拆分并转换为键值对，键为航班号，值为整行数据
      val mapRDD = lineRDD.map(line => {
        (line.split("\t")(0), line)
      })

      // 按航班号分组
      val groupRDD = mapRDD.groupByKey()

      // 组内按时间排序
      val sortedRDD = groupRDD.flatMap {
        case (flightNumber, lines) =>
          lines.toList.sortBy(line => line.split("\t")(13)) // 日期在第14列
      }

      // 将排序后的数据转换为Row类型的RDD
      val rowRDD = sortedRDD.map(lines => {
        val arr = lines.split("\t")
        Row(arr(0), arr(3), arr(4), arr(5), arr(2), arr(11), arr(12), arr(13), arr(14), arr(15))
      })

      // 定义DataFrame的Schema
      val schema = StructType(
        List(
          StructField("flight_number", StringType), // 航班号
          StructField("departure_city", StringType), // 出发地
          StructField("arrival_city", StringType), // 目的地
          StructField("flight_distance", StringType), // 飞行里程
          StructField("flight_type", StringType), // 飞机型号
          StructField("departure_airport", StringType), // 出发机场
          StructField("arrival_airport", StringType), // 到达机场
          StructField("date", StringType), // 日期
          StructField("passenger_count", StringType), // 乘客数量
          StructField("on_time_rate", StringType) // 准点率
        )
      )

      // 创建DataFrame
      val df = sqlContext.createDataFrame(rowRDD, schema)

      // 准备连接MySQL数据库的属性配置
      val prop = new Properties()
      prop.put("user", "root") // 数据库用户名
      prop.put("password", "123456") // 数据库密码
      prop.put("driver", "com.mysql.cj.jdbc.Driver") // JDBC驱动类名

      // 将DataFrame写入MySQL数据库的flight_data表中
      df.write.jdbc("jdbc:mysql://localhost:3306/test01", "flight_data_4", prop)
    }
}