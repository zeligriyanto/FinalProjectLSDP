package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object greedy_matching {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: greedy_matching <input_csv> <output_dir>")
      System.exit(1)
    }

    val inputPath = args(0)  // example: gs://your-bucket/input.csv
    val outputDir = args(1)  // example: gs://your-bucket/output_dir/

    val startTime = System.currentTimeMillis() // <-- Start timer

    val spark = SparkSession.builder()
      .appName("DegreeBasedGreedyMatching")
      .getOrCreate()
    val sc = spark.sparkContext

    // Read input edges from CSV
    val edges: RDD[(Int, Int)] = spark.read
      .option("header", "false")
      .csv(inputPath)
      .rdd
      .map { row =>
        val u = row.getString(0).trim.toInt
        val v = row.getString(1).trim.toInt
        if (u < v) (u, v) else (v, u)
      }
      .distinct()
      .persist()

    // Compute degree of each vertex
    val degrees: Map[Int, Int] = edges.flatMap { case (u, v) => Seq((u, 1), (v, 1)) }
      .reduceByKey(_ + _)
      .collect()
      .toMap

    // Collect and sort edges based on minimum degree
    val sortedEdges: RDD[(Int, Int)] = edges.map { case (u, v) =>
      val minDegree = math.min(degrees.getOrElse(u, 0), degrees.getOrElse(v, 0))
      ((u, v), minDegree)
    }
    .sortBy(_._2)
    .map(_._1)

    // Perform greedy matching
    val matchedVertices = sc.broadcast(scala.collection.mutable.Set[Int]())
    val matchedEdges = sortedEdges.filter { case (u, v) =>
      val set = matchedVertices.value
      if (!set.contains(u) && !set.contains(v)) {
        set += u
        set += v
        true
      } else {
        false
      }
    }

    // Save matched edges into output directory as CSV
    matchedEdges.coalesce(1)
      .map { case (u, v) => s"$u,$v" }
      .saveAsTextFile(outputDir)

    spark.stop()

    val endTime = System.currentTimeMillis() // <-- End timer
    val totalMillis = endTime - startTime
    val totalSeconds = totalMillis / 1000.0

    println(f"Program completed in $totalMillis ms (${totalSeconds}%.2f seconds)")
  }
}