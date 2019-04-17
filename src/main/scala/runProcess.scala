import org.apache.spark.{SparkConf, SparkContext}

object runProcess extends App {

  //set up the spark configuration and create contexts
  val sparkConf = new SparkConf().setAppName("appTornikMap").setMaster("local")
  // your handle to SparkContext to access other context like SQLContext
  val sc = new SparkContext(sparkConf)

  //load log file on rdd
  val rddTornikMap = sc.textFile("src/main/ressource/tornik-map-20171006.10000.tsv")
  //rddTornikMap.collect().map(line => println(line))

  val rddTuileDePlan = rddTornikMap.filter(line => line.contains("/map/1.0"))
  //rddTuileDePlan.collect().map(line => printf(line))

  //************************* First Part **********************************
  val pairRDDViewmode = rddTuileDePlan.map { x =>
    val arrayLine = x.split("/")
    (arrayLine(4), 1)
  }
  //pairRDDViewmode.foreach(x => println(x._1 + " " + x._2))

  val pairRddViewModeOcc = pairRDDViewmode.reduceByKey((x1, x2) => (x1 + x2))
  //pairRddViewModeOcc.foreach(x => println(x._1 + "  " + x._2))

  //************************* Second  Part **********************************
  val pairRddLevelZoom = rddTuileDePlan.map { line =>
    val arrayLine = line.split("/")
    (arrayLine(4), arrayLine(6))
  }.distinct()
  //pairRddLevelZoom.foreach(x => println(x._1 + "  " + x._2))

  val pairRddLevelZoomForMode = pairRddLevelZoom.reduceByKey { (x1, x2) => x1 + ";" + x2 }
  pairRddLevelZoomForMode.foreach(x => println(x._1 + "   " + x._2))

  //************************* third Part *************************************
  val resultParseLog = pairRddViewModeOcc.join(pairRddLevelZoomForMode)
  resultParseLog.foreach(x => println(x._1 + "    " + x._2._1 + "    " + x._2._2))
}



