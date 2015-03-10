// Compute the frequency-of-occurence for every word in a text file,
// and then sort ascending by frequency
// Usage: run in spark-shell with `:load wordcount.scala`

// Get the text file from filesystem
val textFile = sc.textFile("/vagrant_data/macbeth.txt")

// Split into words, map and reduce to tuples like ('thou', 12)
val wordCounts = textFile.flatMap(line => line.split(" ")).  // split the lines into words 
	map(word => (word, 1)).  // put every word in a (word, 1) tuple
	reduceByKey((a, b) => a + b)  // add up all the 1's for every word

// Swap the (word, count) tuple elements so we can sort by count
val switched = wordCounts.map( x => (x._2, x._1))

// Sort and print
switched.sortByKey(true).collect.foreach(println)


