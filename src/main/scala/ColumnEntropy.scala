
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.hive._
import math.{log => mlog} 

import scala.collection.{Map => Map}

object ColumnEntropy {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ColumnEntropy <column1> <column2> <table>")
      System.exit(1)
    }

    val first_column_name = args(0)
    val second_column_name = args(1)
    val table = args(2)

    val conf = new SparkConf().setAppName("Calculate column entropy")
    val sc = new SparkContext(conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val queries = Map(first_column_name -> s"select cast($first_column_name as STRING), count(cast($first_column_name as STRING)) from $table group by $first_column_name", second_column_name -> s"select cast($second_column_name as STRING), count(cast($second_column_name as STRING)) from $table group by $second_column_name")

    var results = Map[String, Double]() 

    val entropies = queries.map((q) => {
      val key = q._1
      val query = q._2

      val counts = hiveContext.hql(query).map(r => { (r.getString(0) -> r.getLong(1))}).collectAsMap()

      var total = 0L
      counts.foreach(total += _._2)
      -counts.map((n) => {
        val p = n._2 / (total + 0.0)
        if (key == second_column_name) {
          results = results + (n._1 -> p)
        }
        p * mlog(p)
      }).fold(0.0)(_ + _)
    })


    // mutual information

    val query = s"select cast($first_column_name as STRING), cast($second_column_name as STRING), count(cast($second_column_name as STRING)) from $table group by $first_column_name, $second_column_name"

    val counts = hiveContext.hql(query).map(r => { (r.getString(0), r.getString(1), r.getLong(2))}).collect()
    var total = 0L
    counts.foreach(total += _._3)
    val cond_entropy = counts.map((n) => {
      val key_y = n._2
      val value = n._3
      val p = value / (total + 0.0)
      p * mlog((results(key_y)) / p)
    }).fold(0.0)(_ + _)
 
    var index = 0
    entropies.foreach((entropy) => {println(s"column: ${args(index)} entropy = $entropy"); index += 1})

    val mi = entropies.head - cond_entropy
    println(s"Mutual information: $mi")

  }
}

