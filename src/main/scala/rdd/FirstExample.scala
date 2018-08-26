package rdd

import org.apache.spark.{SparkConf, SparkContext}

object FirstExample extends App {

  // spark configuration
  val conf = new SparkConf().setAppName("FirstExample").setMaster("local[*]")

  // spark context
  val sc = new SparkContext(conf)

  // data in form of any collection
  val list = List(1, 2, 3, 4)

  // load initial RDD
  val rdd = sc.parallelize(list)

  // transformation
  val transformedRdd = rdd.filter(_ > 2).map(_ * 2)

  // action
  val actionRdd = transformedRdd.count()

  println("\n\nNumber of elements retrieved => " + actionRdd + "\n\n")

}
