
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.hive._
import math.{log => mlog} 

import scala.collection.{Map => Map}

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

    val queries = Map(first_column_name -> s"select cast($first_column_name as STRING), count($count_column_name) from $table group by $first_column_name", second_column_name -> s"select cast($second_column_name as STRING), count($count_column_name) from $table group by $second_column_name")

    var results = Map[String, Map[String, Long]]() 

    val entropies = queries.map((q) => {
      val key = q._1
      val query = q._2

      val counts = hiveContext.hql(query).map(r => { (r.getString(0) -> r.getLong(1))}).collectAsMap()
      results = results + (key -> counts)

      var total = 0L
      counts.foreach(total += _._2)
      -counts.mapValues((n) => { val p = n / (total + 0.0); p * mlog(p) }).values.fold(0.0)(_ + _)
    })


    // mutual information

    val query = s"select cast($first_column_name as STRING), cast($second_column_name as STRING), count($count_column_name) from $table group by $first_column_name, $second_column_name"

    val counts = hiveContext.hql(query).map(r => { (r.getString(0), r.getString(1), r.getLong(2))}).collect()
    var total = 0L
    counts.foreach(total += _._3)
    val cond_entropy = counts.map((n) => {
      val key_y = n._2
      val value = n._3
      val p = value / (total + 0.0)
      p * mlog((results(second_column_name)(key_y)) / p)
    }).fold(0.0)(_ + _)
 
    var index = 0
    entropies.foreach((entropy) => {println(s"column: ${args(index)} entropy = $entropy"); index += 1})
 
    val mi = entropies.head - cond_entropy
    println(s"Mutual information: $mi")

  }
}

