// Get the text file from filesystem
val textFile = sc.textFile("/vagrant_data/macbeth.txt")

// Split into words, map and reduce to tuples like ('thou', 12)
val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

// Swap the (word, count) tuple elements so we can sort by count
val switched = wordCounts.map( x => (x._2, x._1))

// Sort and print
switched.sortByKey(true).collect.foreach(println)

// s = "979509 - - [24/Jun/1998:15:29:34 +0000] \"GET /english/competition/matchstat8862.htm HTTP/1.0\" 200 18268"
// val pattern = """GET (.+) HTTP""".r
// pattern.findAllIn(s).matchData.next.group(1)

// wordCounts.sample(false, .01)


