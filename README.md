# CustomerPurchaseDataAnalyzer
This repository contains Apache Spark code for analyzing how much purchase a Customer has made over a period of time

To execute the file , create a folder structure as follows -
<CURRENT_DIRECTORY>
  CustomerPurchaseDataAnalyzer
    src
      main
        scala
          CustomerPurchaseDataAnalyzer.scala
    built.sbt

Then make a package by executing command - 
  sbt package
  
Then run the package by -
  spark-submit --class CustomerPurchaseDataAnalyzer customer_purchase_analyzer_<version>.jar <input file path> <customer info file name> <customer purchase data file name> <purchase amount threshold>

