/**
  * Created by tanishka on 4/12/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
object DRDayDept {

def main(args : Array[String]): Unit ={

  val prop = ConfigFactory.load()
  val conf = new SparkConf().setMaster(prop.getConfig(args(2)).getString("executionMode")).setAppName("DRDayDept")
  val sc = new SparkContext(conf)

  val fs=FileSystem.get(sc.hadoopConfiguration)

  val inputPathDir=args(0)
  val outputPathDir=args(1)

  val inputPathExists=fs.exists(new Path(inputPathDir))
  val outputPathExists=fs.exists(new Path(outputPathDir))

  if(!inputPathExists)
    {
      println("Input Path doesn't exists")
      return
    }
  if(outputPathExists)
    {
      fs.delete(new Path(outputPathDir),true)
    }


  val departments=sc.textFile(inputPathDir+"departments")
  val categories=sc.textFile(inputPathDir+"categories")
  val products=sc.textFile(inputPathDir+"products")
  val orderItems=sc.textFile(inputPathDir+"order_items")
  val orders=sc.textFile(inputPathDir+"orders")

  val departmentsMap=departments.map(rec=>(rec.split(",")(0).toInt,rec.split(",")(1)))
  val categoriesMap=categories.map(rec=>(rec.split(",")(0).toInt,rec.split(",")(1)))
  val productsMap=products.map(rec=>(rec.split(",")(1).toInt,rec.split(",")(0)))

  val prodCatJoin=productsMap.join(categoriesMap)

  val prodCatJoinMap=prodCatJoin.map(rec=>(rec._2._2.toInt,rec._2._1))

  val orderItemsMap=orderItems.map(rec=>(rec.split(",")(1).toInt,(rec.split(",")(2),rec.split(",")(4))))

  val ordersMap=orders.filter(rec=>{rec.split(",")(3)=="COMPLETE"|rec.split(",")(3)=="CLOSED"}).map(rec=>(rec.split(",")(0).toInt,rec.split(",")(1)))

  val ordersJoinOrderItems= orderItemsMap.join(ordersMap)


  val ordersJoinOrderItemsMap=ordersJoinOrderItems.map(rec=>(rec._2._1._1.toInt,(rec._2._2,rec._2._1._2)))

  val prodJoinDept=prodCatJoinMap.join(departmentsMap).distinct

  val prodJoinDeptMap = prodJoinDept.map(rec=>(rec._2._1.toInt,rec._2._2))

  val prodJoinOrders=ordersJoinOrderItemsMap.join(prodJoinDeptMap)

  val prodJoinOrdersMap=prodJoinOrders.map(rec=>((rec._2._1._1,rec._2._2),rec._2._1._2.toFloat)).reduceByKey(_+_).sortByKey()

 prodJoinOrdersMap.saveAsTextFile(outputPathDir)


}

}
