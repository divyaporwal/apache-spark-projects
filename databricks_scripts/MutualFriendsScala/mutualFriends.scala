  val txtFile = sc.textFile("/FileStore/tables/socAdj.txt")
  val result = txtFile.flatMap(Map)
    .reduceByKey(getIntersection)
    .filter(!_._2.equals("null")).filter(!_._2.isEmpty)
    .sortByKey()
  val res = result.map(value => value._1 + "\t" + value._2.size)
  res.saveAsTextFile("/FileStore/tables/part1")

  def getIntersection(first: Set[String], second: Set[String]) = {
    first.toSet.intersect(second.toSet)
  }

  def Map(line: String) = {
    val cline = line.split("\\t+")
    val person = cline(0)
    val new_friends = if (cline.length > 1) {
      cline(1) 
    } else "null"
    val friendsArr = new_friends.split(",")
    val friends = for (i <- 0 to friendsArr.length - 1) yield friendsArr(i)
    
    val pairs = friendsArr.map(friend => {
      if (person < friend) person + "," + friend else friend + "," + person
    })
    pairs.map(pair => (pair, friends.toSet))
  }