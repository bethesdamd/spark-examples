// Spark demo: processing world cup http logs
// Input looks like this:
// 979509 - - [24/Jun/1998:15:29:34 +0000] "GET /english/competition/matchstat8862.htm HTTP/1.0" 200 18268

import org.apache.spark.SparkContext._

val file = sc.textFile("/vagrant_data/world_cup_200k")

val pattern = """GET (.+) HTTP""".r   // Regular expression to extract url

// From the raw data RDD create a new RDD containing tuples of type (url, 1)
val urls = file.map { line =>
	val md = pattern.findAllIn(line).matchData
	if (!md.isEmpty) {
		(md.next.group(1).toString, 1)
	} else {
		("parsefailed", 1)
	}
}
// Remove uninteresting files
val filtered = urls.filter(tup  => 
	!tup._1.contains(".gif") && 
	!tup._1.contains(".jpg") && 
	!tup._1.contains(".js") &&
	!tup._1.contains(".class") &&
	!tup._1.contains("_inet"))
  
val r = filtered.reduceByKey((a,b) => a + b)  // tabulate url counts e.g. (/home.html, 4121)
r.take(10).foreach(println)  // print out some intermediate results

// swap the url (key) with the count (value) so we can use sortByKey on the count
val swap = r.map(tup => (tup._2, tup._1))  
val sorted = swap.sortByKey(false)

// sorted is url and frequency sorted by frequency descending
sorted.take(15).foreach(println)

