
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.hive._
import math.{log => mlog} 

object ColumnEntropy {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: ColumnEntropy <column1> <column2> <count column> <table>")
      System.exit(1)
    }

    val first_column_name = args(0)
    val second_column_name = args(1)
    val count_column_name = args(2)
    val table = args(3)

    val conf = new SparkConf().setAppName("Calculate column entropy")
    val sc = new SparkContext(conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val queries = s"select cast($first_column_name as STRING), count($count_column_name) from $table group by $first_column_name" :: s"select cast($second_column_name as STRING), count($count_column_name) from $table group by $second_column_name" :: List()

    val entropies = queries.map((query) => {
      val counts = hiveContext.hql(query).map(r => { (r.getString(0) -> r.getLong(1))}).collectAsMap()
      var total = 0L
      counts.foreach(total += _._2)
      counts.mapValues((n) => { val p = n / (total + 0.0); p * 1 / mlog(p) }).values.fold(0.0)(_ + _)
    })

    var index = 0
    entropies.foreach((entropy) => {println(s"column: ${args(index)} entropy = $entropy"); index += 1})
  }
}

