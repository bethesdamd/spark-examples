/*
 Apache Spark GraphX processing example
 Create a graph from twitter data and calculate the connected components and the triangle counts
 See https://spark.apache.org/docs/1.2.1/graphx-programming-guide.html for more details about GraphX
 and these operations
*/

import org.apache.spark.graphx.Graph
val raw = sc.textFile("50krows.txt")

// Extract the two comma-delimited fields, and create a new tuple (src,dst) where src is source node,
// dst is destination node, and the ordering is src < dst.  This ordering is necessary for some GraphX operations
val data = raw.map { x => 
	val s = x.split(",")
	val a = s(0).toLong
	val b = s(1).toLong
	if (a > b) (b,a) else (a,b)
}
val graph = Graph.fromEdgeTuples(data, "v")  // Create the graph
val cc = graph.connectedComponents()  // Calculate the connected components

// Find the triangle count for each vertex, then put the count first and the vertex id second:
val tri = graph.triangleCount.vertices.map( x => (x._2, x._1))

// Sort by triangle count.  This gives us a list of vertices with their triangle count, ordered by 
// triangle count, so we can easily see which vertices have the largest count
val toptris = tri.sortByKey(false)
toptris.saveAsTextFile("/tmp/triangles")






