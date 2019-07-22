import org.apache.spark.graphx.{Graph => Graph1, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Graph {
def main(args:Array[String])
{
 val conf = new SparkConf().setAppName("Assignment 8")
 val sc = new SparkContext(conf) 
 val edges: RDD[Edge[Long]] = sc.textFile(args(0)).map( line => { val (node, neighbours) = line.split(",").splitAt(1) 
 												(node(0).toLong,neighbours.toList.map(_.toLong)) } )
												.flatMap( x => x._2.map(y => (x._1, y)))
												.map(nodes => Edge(nodes._1, nodes._2, nodes._1))
val graphnew : Graph1[Long,Long] = Graph1.fromEdges(edges, "defaultProperty") .mapVertices((id, _) => id)
									
	val preg = graphnew.pregel(Long.MaxValue, 5) ( (id, oldGrp, newGrp) => math.min(oldGrp, newGrp), // Graph pregel method and Vertex Program
	triplet => {  // triplet method and Send Message
        if (triplet.attr < triplet.dstAttr) 
        {

          Iterator((triplet.dstId, triplet.attr))//edge.attr,edge.dstID

        } else if (triplet.srcAttr <  triplet.attr) 
        {
          
          Iterator((triplet.dstId, triplet.srcAttr)) //edge.srcattr,edge.dstID

        } else 
        {

			Iterator.empty
		}

      }, (a, b) => math.min(a, b) // finiding the minimum key and merge message
    )

   val result = preg.vertices.map(graphnew => (graphnew._2, 1))
					.reduceByKey(_ + _)
					.sortByKey()
					.collect()
					//.groupByKey()
					.map(k => k._1.toString+" "+k._2.toString)
	result.foreach(println)							
    
}
}