// Spark demo: processing world cup http logs.  Input looks like this:
// 979509 - - [24/Jun/1998:15:29:34 +0000] "GET /english/competition/matchstat8862.htm HTTP/1.0" 200 18268
// First field is a unique client identifier that is a surrogate for an IP address

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

// Laod a text file from filesystem or HDFS and create an RDD
val file = sc.textFile("/vagrant_data/world_cup_200k")

// From the raw data RDD create a new RDD containing tuples
// Accepts the RDD and the parsing regexp; returns RDD containing tuples like (String, 1)
// where String is whatever the regexp extracted, e.g. a URL or IP address, etc.
def myparse(rdd: RDD[String], regex:Regex):RDD[(String, Int)] = rdd.map { line =>
	val md = regex.findAllIn(line).matchData
	val s = line.split(" ")
	if (!md.isEmpty && s.size == 10) {
		(md.next.group(1).toString, 1)
	} else {
		("parsefailed", 1)
	}
}

val pattern = """GET (.+) HTTP""".r   // Regular expression to extract url
val urls = myparse(file, pattern)

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

// sorted is url and frequency sorted by frequency descending.  This shows the most popular url's.
sorted.take(15).foreach(println)

val pattern = """^([\w\-]+)""".r   // Regular expression to extract client_id (first 'word' in the line)

// requestors = (client_id, 1)
// (org.apache.spark.rdd.RDD[(String, Int)] ((926664,1), (1264965,1), (677046,1) ... )
val requests = myparse(file, pattern)

// Which clients made the most requests?
val most_client_requests = requestors.reduceByKey(_ + _).  // sum all the requests, by client 
	map(x => (x._2, x._1)).  // swap the values so we can sort by key in next step
	sortByKey(false) // sort by number of requests, descending
	
