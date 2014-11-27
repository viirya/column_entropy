
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.hive._
import math.{log => mlog} 

import java.io.File
import java.io.FileOutputStream

import scala.collection.{Map => Map}

object ColumnPairMI {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ColumnPairMI <column1> <column2> <table>")
      System.exit(1)
    }
 
    val first_column_name = args(0)
    val second_column_name = args(1)
    val table = args(2)

    val mi_calculator = new ColumnMI()

    val mi = mi_calculator.getEntropy(Array(first_column_name, second_column_name), table)
    println(s"Final result: $mi")
  }

}

object ColumnPairsMI {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: ColumnPairsMI <table> <from> <until>")
      System.exit(1)
    }
 
    val table = args(0)
    val from: Int = if (args.length > 1) args(1).toInt else 0

    // if until is not greater than 0, then we will calculate all combinations
    val until: Int = if (args.length > 2) args(2).toInt else -1

    val mi_calculator = new ColumnMI()
    val columns_entropies = mi_calculator.getColumnsEntropies(table, from, until)
    var index = 0

    columns_entropies._2.foreach((entropy) => {
        if (entropy != 0.0) {
          val col_pair = columns_entropies._1(index)
          println(s"columns: ${col_pair(0)} ${col_pair(1)} mi: $entropy")
        }
        index += 1
    })
  }

}

object ColumnPairsMIExport {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ColumnPairsMIExport <table> <export table>")
      System.exit(1)
    }
 
    val table = args(0)
    val export_table = args(1)

    val mi_calculator = new ColumnMI()
    val columns_entropies = mi_calculator.getColumnsEntropies(table, 0, -1)
    mi_calculator.exportToHiveTable(export_table, columns_entropies)

  }
} 
 
class ColumnMI {

  val conf = new SparkConf().setAppName("Calculate column entropy")
  val sc = new SparkContext(conf)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

  def createTempOutput(): Option[File] = {
    try {
      return Some(File.createTempFile("column_mi_", ".tmp"))
    } catch { case e: Exception => return None }
  }

  def exportToHiveTable(table: String, columns_entropies: Tuple2[Array[Array[String]], Array[Double]]) = {

    createTempOutput() match {
      case Some(file: File) =>
        file.deleteOnExit()
        val output = new FileOutputStream(file)

        var index = 0
        columns_entropies._2.foreach((entropy) => {
            if (entropy != 0.0) {
              val col_pair = columns_entropies._1(index)
              println(s"columns: ${col_pair(0)} ${col_pair(1)} mi: $entropy")
              output.write(s"${col_pair(0)}\t${col_pair(1)}\t$entropy\n".getBytes())
            }
            index += 1
        })
        output.close()

        hiveContext.hql(s"DROP TABLE $table")
        hiveContext.hql(s"CREATE TABLE $table (col_from STRING, col_to STRING, mi DECIMAL(9, 7)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'")

        hiveContext.hql(s"LOAD DATA LOCAL INPATH '${file.getAbsolutePath()}' OVERWRITE INTO TABLE $table")

      case None =>
    }
  }

  def getColumnList(table: String): Array[String] = {

    val fields = hiveContext.hql(s"describe $table").map(r => { r.getString(0) }).collect() 

    return fields
 
  }

  def getColumnsEntropies(table: String, from: Int, until: Int): Tuple2[Array[Array[String]], Array[Double]] = {

    var columns = getColumnList(table).combinations(2).toArray

    if (until > 0) {
      columns = columns.slice(from, until)
    }

    val mis = columns.map((column_pair) => {
      if (column_pair(0) != "__presto__sample_weight__" && column_pair(1) != "__presto__sample_weight__")
        getEntropy(column_pair, table)
      else
        0.0
    })

    return (columns, mis)
  }

  def getEntropy(columns: Array[String], table: String): Double = {
 
    val first_column_name = columns(0)
    val second_column_name = columns(1)
 
    val queries = Map(first_column_name -> s"select cast($first_column_name as STRING), count(cast($first_column_name as STRING)) from $table group by $first_column_name", second_column_name -> s"select cast($second_column_name as STRING), count(cast($second_column_name as STRING)) from $table group by $second_column_name")

    var results = Map[String, Double]() 

    val entropies = queries.map((q) => {
      val key = q._1
      val query = q._2

      val counts = hiveContext.hql(query).map(r => { if (r.isNullAt(0)) ("NULL" -> r.getLong(1)) else (r.getString(0) -> r.getLong(1))}).collectAsMap()

      var total = 0L
      counts.foreach((n) => { if (n._1 != "NULL") total += n._2 })
      -counts.map((n) => {
        if (n._1 != "NULL") {
          val p = n._2 / (total + 0.0)
          if (key == second_column_name) {
            results = results + (n._1 -> p)
          }
          p * mlog(p)
        } else {
          0.0
        }
      }).fold(0.0)(_ + _)
    })


    // mutual information

    val query = s"select cast($first_column_name as STRING), cast($second_column_name as STRING), count(cast($second_column_name as STRING)) from $table group by $first_column_name, $second_column_name"

    val counts = hiveContext.hql(query).map(r => { if (r.isNullAt(0) || r.isNullAt(1)) ("NULL", "NULL", r.getLong(2)) else (r.getString(0), r.getString(1), r.getLong(2))}).collect()
    var total = 0L
    counts.foreach((n) => { if (n._1 != "NULL") total += n._3 })
    val cond_entropy = counts.map((n) => {
      if (n._1 != "NULL") {
        val key_y = n._2
        val value = n._3
        val p = value / (total + 0.0)
        p * mlog((results(key_y)) / p)
      } else {
        0.0
      }
    }).fold(0.0)(_ + _)
 
    var index = 0
    entropies.foreach((entropy) => {println(s"column: ${columns(index)} entropy = $entropy"); index += 1})

    val mi = entropies.head - cond_entropy
    println(s"Mutual information: $mi")

    return mi

  }
}

