package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.util.Random

object luby_matching {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: luby_matching <input_csv> <output_dir>")
      System.exit(1)
    }

    val inputPath = args(0)  // e.g., "gs://your-bucket/input.csv"
    val outputDir = args(1)  // e.g., "gs://your-bucket/output_dir/"

    val startTime = System.currentTimeMillis() // <-- Start timer

    val spark = SparkSession.builder()
      .appName("LubyMaximalMatching")
      .getOrCreate()
    val sc = spark.sparkContext

    // Read input edges from GCS CSV
    var edges: RDD[(Int, Int)] = spark.read
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

    var matchedVertices = sc.broadcast(Set[Int]())
    var finalMatches = sc.emptyRDD[(Int, Int)]

    var round = 0
    var continue = true

    while (continue) {
      round += 1
      println(s"Starting round $round with ${edges.count()} edges")

      // Filter out edges where either endpoint is already matched
      val availableEdges = edges.filter { case (u, v) =>
        !matchedVertices.value.contains(u) && !matchedVertices.value.contains(v)
      }
      availableEdges.persist()

      if (availableEdges.isEmpty()) {
        continue = false
      } else {
        // Assign random priorities to edges
        val prioritizedEdges = availableEdges.map { case (u, v) =>
          ((u, v), Random.nextDouble())
        }

        // Each vertex picks its best (lowest priority) adjacent edge
        val vertexChoices = prioritizedEdges.flatMap { case ((u, v), priority) =>
          Seq((u, ((u, v), priority)), (v, ((u, v), priority)))
        }
          .reduceByKey { case ((e1, p1), (e2, p2)) =>
            if (p1 < p2) (e1, p1) else (e2, p2)
          }

        // For each edge, count how many vertices chose it
        val edgeProposals = vertexChoices.map { case (vertex, ((u, v), priority)) =>
          ((u, v), 1)
        }
          .reduceByKey(_ + _)

        // Keep edges proposed by both endpoints
        val newMatches = edgeProposals.filter { case ((u, v), count) => count == 2 }
          .map { case ((u, v), _) => (u, v) }

        newMatches.persist()

        val matchedVertsNow = newMatches.flatMap { case (u, v) => Seq(u, v) }.distinct()

        // Update matched vertices
        matchedVertices = sc.broadcast(
          matchedVertices.value ++ matchedVertsNow.collect().toSet
        )

        // Add new matches to final matches
        finalMatches = finalMatches.union(newMatches)

        println(s"Round $round: matched ${newMatches.count()} new edges")

        // Stop if no new matches were made
        if (newMatches.isEmpty()) {
          continue = false
        }
      }

      availableEdges.unpersist()
    }

    println(s"Finished Luby matching in $round rounds with total ${finalMatches.count()} edges matched.")

    // Save final matches as CSV
    finalMatches.coalesce(1)
      .map { case (u, v) => s"$u,$v" }
      .saveAsTextFile(outputDir)

    spark.stop()

    val endTime = System.currentTimeMillis() // <-- End timer
    val totalMillis = endTime - startTime
    val totalSeconds = totalMillis / 1000.0

    println(f"Program completed in $totalMillis ms (${totalSeconds}%.2f seconds)")
  }
}