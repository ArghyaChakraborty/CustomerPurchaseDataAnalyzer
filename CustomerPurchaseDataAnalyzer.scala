import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CustomerPurchaseDataAnalyzer {
  
  def main(args : Array[String]) {
    if(args.length < 4) {
      println("Usage : spark-submit --class CustomerPurchaseDataAnalyzer customer_purchase_analyzer_<version>.jar <input file path> <customer info file name> <customer purchase data file name> <purchase amount threshold>")
    } else {
      // Creating Spark Context
      val sconf = new SparkConf().setAppName("Customer Purchase Data Analyzer").setMaster("local")
      val sc = new SparkContext(sconf)
      
      // Reading both the input files 
      val customerDataFile = sc.textFile(args(0)+args(1))
      val customerPurchaseDataFile = sc.textFile(args(0)+args(2))
      
      // And the theashold value as Float
      val purchaseThresholdFloat = args(3).toFloat
      
      // Transforming the Customer Purchase Data RDD to get Total Purchase Amount of each customer 
      val totalCustPurchase = customerPurchaseDataFile.map(_.split(",")).filter(x => !x(0).startsWith("customer")).map(x => (x(0),x(1).toFloat)).reduceByKey((a,b) => a+b)
      // Transforming the previous RDD to collect only the Customer Data crossing threshold purchase amount
      val totalCustomerPurchaseRdd = totalCustPurchase.filter(x => x._2 > purchaseThresholdFloat)
      
      // Transforming Customer Data into K/V Pairs
      val customerDataRdd = customerDataFile.map(_.split(",")).map(a => (a(0),(a(1),a(2))))
      
      // Joinng the Customer Data and Customer Purchase Data (crossing threshold amt) K/V Pair RDDs
      val customerCrossingThreshold = customerDataRdd join totalCustomerPurchaseRdd
      
      // println("Customer Crossing Threshold Amount :")
      customerCrossingThreshold.foreach(println)
      
      sc.stop()
    }
  }

}