/*
 Apache Spark GraphX processing example
 Create a graph from twitter data and calculate the connected components and the triangle counts
 See https://spark.apache.org/docs/0.9.0/graphx-programming-guide.html for more details about GraphX
 and these operations
*/

import org.apache.spark.graphx.Graph
val raw = sc.textFile("50krows.txt")
val data = raw.map { x => 
	val s = x.split(",")
	val a = s(0).toLong
	val b = s(1).toLong
	if (a > b) (b,a) else (a,b)
}
val graph = Graph.fromEdgeTuples(data, "v")
val cc = graph.connectedComponents()

// Find the triangle count for each vertex, then put the count first and the vertex id second:
val tri = graph.triangleCount.vertices.map( x => (x._2, x._1))
val toptris = tri.sortByKey(false)






