import org.apache.spark.rdd.RDD

// 把普通RDD转换为Paired RDD
import java.security.MessageDigest
 
// 这里的下划线"_"是占位符，代表数据文件的根目录
val rootPath: String = "/Users/zhiyu9/Documents/github/learn-spark/chapter01"
val file: String = s"${rootPath}/wikiOfSpark.txt"

// 读取文件内容
val lineRDD: RDD[String] = spark.sparkContext.textFile(file) 

// 以行为单位做分词
val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" ")) 

// 过滤掉空字符串
val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))

val kvRDD: RDD[(String, Int)] = cleanWordRDD.map{ word =>
  // 获取MD5对象实例
  val md5 = MessageDigest.getInstance("MD5")
  // 使用MD5计算哈希值
  val hash = md5.digest(word.getBytes).mkString
  // 返回哈希值与数字1的Pair
  (hash, 1)
}

// 按照单词做分组计数
val hashCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y) 

// 打印词频最高的5个hash
hashCounts.map{case (k, v) => (v, k)}.sortByKey(false).take(5)

