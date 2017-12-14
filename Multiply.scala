import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {
  def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("Matrix Multiplication")
    val sc = new SparkContext(conf)      
    val m = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                (a(0).toInt,a(1).toInt,a(2).toDouble) } )
    val n = sc.textFile(args(1)).map( line => { val a = line.split(",")
                                                (a(0).toInt,a(1).toInt,a(2).toDouble) } )
     
    val intermediate = m.map( m => (m._2,(0,m._1,m._3)) ).join(n.map( n => (n._1,(1,n._2,n._3)) ))
                .map { case (k,(m,n)) => m._2+","+n._2+","+m._3*n._3 }

    
    val result = intermediate.map( line => { val a = line.split(",")
                                             (a(0).toInt,a(1).toInt,a(2).toDouble) } ).
                                             map( res => ((res._1,res._2),res._3) ).
                                             reduceByKey((x,y) => x+y ).sortByKey().map { case (x,y) => x._1+","+x._2+","+y}
    
    result.collect().foreach ( println)  
    result.saveAsTextFile(args(2))
    sc.stop()
  }
}
