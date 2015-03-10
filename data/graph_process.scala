// GraphX processing example
import org.apache.spark.graphx.Graph
// import org.apache.spark
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






