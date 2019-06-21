package net.sansa_stack.examples.spark.hdt

import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.query.spark.query._
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}


class TripleOpsQuery
{
  val log=LoggerFactory.getLogger("TripleOpsQueryy")
  def parseValue(value:String): String ={
    value.replace("<","").replace(">","")
  }

  /*
  Function Converts SPQRQL projection fields to SQL Projection
   */
  def getProjectionFields (query: String)  = {

    val projectionField=query.substring(query.toLowerCase().indexOf("select")+6,query.toLowerCase().indexOf("where"))
    val columnList=projectionField.split("\\s+")
    var result=""
    for(col <- columnList){
      if(col.toLowerCase().contains("?s")){
        result+=s"${TripleOps.SUBJECT_TABLE}.name as subject, "
      }
      else if(col.toLowerCase().contains("?o")){
        result+=s"${TripleOps.OBJECT_TABLE}.name as object, "

      }else if(col.toLowerCase().contains("?p")){
        result+=s"${TripleOps.PREDICATE_TABLE}.name as predicate, "
      }

    }
    //remove extra comma at the end
    result.reverse.replaceFirst(",","").reverse
  }

  /*
  Important function that convert SPARQL Query to SQL
   */
  def getJoinQuery(query:String) ={
    val result=s"select ${getProjectionFields(query)} from ${TripleOps.HDT_TABLE} inner join ${TripleOps.SUBJECT_TABLE} on ${TripleOps.HDT_TABLE}.s=${TripleOps.SUBJECT_TABLE}.index" +
      s" inner join ${TripleOps.OBJECT_TABLE} on ${TripleOps.HDT_TABLE}.o=${TripleOps.OBJECT_TABLE}.index" +
      s" inner join ${TripleOps.PREDICATE_TABLE} on ${TripleOps.HDT_TABLE}.p=${TripleOps.PREDICATE_TABLE}.index" +
      s" ${getWhereCondition(query)} "
    result
  }



/*
Function to convert SparQL where condition to SQL
 */
  def getWhereCondition (query: String) : String = {

    val whereString=query.substring(query.toLowerCase().indexOf("where {")+7,query.toLowerCase().indexOf("}"))
    val conditions=whereString.trim.split(" \\. ")
    var conditionStr=""
    for(condition <- conditions){
      //println("Processing condition: "+condition)
      if(condition.trim.length > 3){
        var tempStr="  "
        //println(" Count: "+condition.trim.split("\\s+").length)
        var subjectCondition=condition.trim.split("\\s+")(0)
        var predicateCondition =condition.trim.split("\\s+")(1)
        var objectCondition=condition.trim.split("\\s+")(2)
        //println(s"Subject Condition: ${subjectCondition}")
        //println(s"Object Condition: ${objectCondition}")
        //println(s"Predicate Condition: ${predicateCondition}")

        if(!subjectCondition.toLowerCase().contains("?s")){
          tempStr += s" ${TripleOps.SUBJECT_TABLE}.name='${parseValue(subjectCondition.trim)}' and"
        }
        if(!objectCondition.toLowerCase().contains("?o")){
          tempStr += s" ${TripleOps.OBJECT_TABLE}.name='${parseValue(objectCondition.trim)}' and"
        }
        if(!predicateCondition.toLowerCase().contains("?p")){
          tempStr += s" ${TripleOps.PREDICATE_TABLE}.name='${parseValue(predicateCondition.trim)}' and"
        }
        //println(tempStr)
        if(tempStr.length>3){
          conditionStr+="( "+ tempStr.reverse.replaceFirst("dna","").reverse + " ) and "
          //println(tempStr.trim.reverse.replaceFirst("and","").reverse)
        }

      }
    }
    conditionStr=conditionStr.reverse.replaceFirst("dna","").reverse
    if(conditionStr.length>5) s" where ${conditionStr}" else ""
  }

}


/*
  OR Where Condition Testing
  AND Where Condition Testing
  Select with Nested Filter
  Query with Join
  Query with Left Join
  Query with Right Join
  Query with Outer Join
 */
object TripleOpsQuery{


  def execute(spark:SparkSession,rdfTriple: RDD[org.apache.jena.graph.Triple] , query:String): Unit ={

    var queryops=new TripleOpsQuery()

    var df=spark.sql(queryops.getJoinQuery(query))
    val count=rdfTriple.sparql(query).count()
    println(s"SparQL Query : ${query}")
    println("Spark SQL: "+queryops.getJoinQuery(query))
    println("SparQL Query Count: "+ count)
    println(s"Spark SQL Count: ${df.count()}")

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TripleOps").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

     val lang = Lang.NTRIPLES
     val rdfTriple = spark.rdf(lang)("small/bsbm/sample.nt");
     val hdtDF = TripleOps.getHDT(rdfTriple)

     //var query="SELECT ?S ?O ?P WHERE { ?S <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual4> ?P .  }"
     var query="SELECT ?S ?O ?P WHERE { ?S ?P ?O }"
    execute(spark,rdfTriple,query)

    query="SELECT ?S ?O ?P WHERE { ?S <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1> ?O .  }"
    execute(spark,rdfTriple,query)


    query="SELECT ?S ?O ?P WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product92> ?P ?O . ?S <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1> ?O . }"
    execute(spark,rdfTriple,query)
  }
}
