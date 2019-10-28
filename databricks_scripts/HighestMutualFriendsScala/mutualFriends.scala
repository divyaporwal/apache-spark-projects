  import scala.collection.mutable.ArrayBuffer

  val columnNames = Seq("_c0","_c1", "_c2", "_c3", "_c4", "_c5")
  val columnNames1 = Seq("_c1", "_c2")
  val txtFile = sc.textFile("/FileStore/tables/socAdj.txt")
  var df = spark.read.format("csv").option("header", "false").load("/FileStore/tables/userdata.txt")
  var fileFriends = sc.textFile("/FileStore/tables/socAdj.txt")
  var result = fileFriends.flatMap(Map)
    .reduceByKey(intersection)
  var res = result.map(value => value._1 + "\t" + value._2.size)
  var incMap = result.map{case (key, value) => (key, value.size)} 
  var sort = incMap.sortBy(_._2,false)
  sort.collect
  var res1 = sort.map(value => value._2 + "," + value._1).take(10)
  var resultArray = ArrayBuffer[String]()
  var st = ""
  for (e <- res1) {
    st = e.split(",")(0) + "\t" + df.filter(df("_c0").equalTo(e.split(",")(1))).select("_c1", "_c2").collect().map(row => row.mkString(",")).mkString(" ") + "\t" + df.filter(df("_c0").equalTo(e.split(",")(1))).select("_c3", "_c4").collect().map(row => row.mkString(",")).mkString(" ") + "\t" + df.filter(df("_c0").equalTo(e.split(",")(2))).select("_c1", "_c2").collect().map(row => row.mkString(",")).mkString(" ") + "\t" + df.filter(df("_c0").equalTo(e.split(",")(2))).select("_c3", "_c4").collect().map(row => row.mkString(",")).mkString(" ")
    resultArray += st
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

  def intersection(first: Set[String], second: Set[String]) = {
    first.toSet.intersect(second.toSet)
  }
