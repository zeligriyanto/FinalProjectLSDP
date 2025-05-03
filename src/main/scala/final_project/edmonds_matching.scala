package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.util.control.Breaks._

object edmonds_matching {

  case class Edge(u: Int, v: Int)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: edmonds_matching <input_csv> <output_dir>")
      System.exit(1)
    }

    val inputPath = args(0)  // e.g., gs://your-bucket/input.csv
    val outputDir = args(1)  // e.g., gs://your-bucket/output_dir/

    val spark = SparkSession.builder()
      .appName("EdmondsMatching")
      .getOrCreate()
    val sc = spark.sparkContext

    // Read input edges from CSV
    val edgesRDD: RDD[(Int, Int)] = spark.read
      .option("header", "false")
      .csv(inputPath)
      .rdd
      .map { row =>
        val u = row.getString(0).trim.toInt
        val v = row.getString(1).trim.toInt
        (u, v)
      }
      .persist()

    val edges = edgesRDD.collect().map { case (u, v) => Edge(u, v) }.toList
    val nodes = edges.flatMap(e => List(e.u, e.v)).toSet

    val graph = buildAdjacencyList(edges)
    val n = if (nodes.nonEmpty) nodes.max + 1 else 0 // Account for non-contiguous labels

    val startTime = System.currentTimeMillis()

    val matching = maximumMatching(graph, n)

    val endTime = System.currentTimeMillis()
    val totalMillis = endTime - startTime
    val totalSeconds = totalMillis / 1000.0

    println(f"Matching completed in $totalMillis ms (${totalSeconds}%.2f seconds)")

    // Write matched edges to output
    val matchedEdgesRDD: RDD[String] = sc.parallelize(
      matching
        .filter { case (u, v) => u < v }
        .map { case (u, v) => s"$u,$v" }
        .toSeq
    )

    matchedEdgesRDD.coalesce(1)
      .saveAsTextFile(outputDir)

    spark.stop()
  }

  def buildAdjacencyList(edges: List[Edge]): Map[Int, Set[Int]] = {
    val graph = mutable.Map[Int, mutable.Set[Int]]()
    for (Edge(u, v) <- edges) {
      graph.getOrElseUpdate(u, mutable.Set()) += v
      graph.getOrElseUpdate(v, mutable.Set()) += u
    }
    graph.mapValues(_.toSet).toMap
  }

  def maximumMatching(graph: Map[Int, Set[Int]], n: Int): Map[Int, Int] = {
    val matchTo = mutable.Map[Int, Int]()
    for (v <- graph.keys) {
      matchTo(v) = -1
    }

    var progressCounter = 0

    for (v <- graph.keys) {
      if (matchTo(v) == -1) {

        val base = (0 until n).toArray
        val parent = Array.fill(n)(-1)
        val used = Array.fill(n)(false)
        val queue = mutable.Queue[Int]()

        def lca(a0: Int, b0: Int): Int = {
          val visited = Array.fill(n)(false)
          var a = a0
          breakable {
            while (true) {
              a = base(a)
              visited(a) = true
              if (matchTo.contains(a) && matchTo(a) != -1) {
                a = parent(matchTo(a))
              } else {
                break()
              }
            }
          }
          var b = b0
          breakable {
            while (true) {
              b = base(b)
              if (visited(b)) return b
              if (matchTo.contains(b) && matchTo(b) != -1) {
                b = parent(matchTo(b))
              } else {
                return b
              }
            }
          }
          b
        }

        def markPath(start: Int, b: Int, child: Int): Unit = {
          var v = start
          var children = child
          while (base(v) != b) {
            used(base(v)) = true
            used(base(matchTo(v))) = true
            parent(v) = children
            children = matchTo(v)
            v = parent(matchTo(v))
          }
        }

        def findAugmentingPath(root: Int): Boolean = {
          used.indices.foreach(i => {
            used(i) = false
            parent(i) = -1
            base(i) = i
          })
          queue.clear()
          queue.enqueue(root)
          used(root) = true

          while (queue.nonEmpty) {
            val v = queue.dequeue()
            for (u <- graph.getOrElse(v, Set.empty)) {
              if (base(v) == base(u) || matchTo(v) == u) {
                // Skip
              } else if (u == root || (matchTo(u) != -1 && parent(matchTo(u)) != -1)) {
                val curbase = lca(v, u)
                val blossomVisited = Array.fill(n)(false)
                markPath(v, curbase, u)
                markPath(u, curbase, v)
                for (i <- 0 until n) {
                  if (used(base(i)) && base(i) != curbase) {
                    base(i) = curbase
                    if (!blossomVisited(i)) {
                      blossomVisited(i) = true
                      queue.enqueue(i)
                    }
                  }
                }
              } else if (parent(u) == -1) {
                parent(u) = v
                if (matchTo(u) == -1) {
                  var cur = u
                  var prev = v
                  while (cur != -1) {
                    val tmp = matchTo(prev)
                    matchTo(prev) = cur
                    matchTo(cur) = prev
                    cur = tmp
                    if (cur != -1) prev = parent(cur)
                  }
                  return true
                } else {
                  queue.enqueue(matchTo(u))
                  used(matchTo(u)) = true
                }
              }
            }
          }
          false
        }

        val matched = findAugmentingPath(v)
        if (matched) {
          progressCounter += 1
          println(s"Progress: $progressCounter matches so far")
        }
      }
    }

    println(s"Finished matching with total ${progressCounter} augmenting paths found.")

    matchTo.toMap.filter { case (u, v) => v != -1 }
  }
}