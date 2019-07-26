package net.sansa_stack.examples.spark.hdt

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.algebra.OpWalker
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.rdd.RDD
import org.apache.jena.sparql.expr.Expr
import net.sansa_stack.query.spark.query._

class TripleOpsQueryNew() {

  val queryScanner = new QueryScanner()

  def getProjectionFields() = {
    var result = ""

    for (i <- 0 to queryScanner.varList.size() - 1) {
      val name=queryScanner.varList.get(i).getVarName

      if(name.equalsIgnoreCase("S")){
        result+=s"${TripleOps.SUBJECT_TABLE}.name as subject, "
      }
      else  if(name.equalsIgnoreCase("O")){
        result+=s"${TripleOps.OBJECT_TABLE}.name as object, "
      }
      else  if(name.equalsIgnoreCase("P")){
        result+=s"${TripleOps.PREDICATE_TABLE}.name as predicate, "
      }
    }
    //remove extra comma at the end
    result.reverse.replaceFirst(",", "").reverse
  }

  def getWhereCondition(): String = {
    var tempStr = ""
    for(i <- 0 to queryScanner.whereCondition.size()-1)
    {
      if(!queryScanner.subjects.get(i).toString().toLowerCase().contains("?s")){
        tempStr += s" ${TripleOps.SUBJECT_TABLE}.name='${queryScanner.subjects.get(i)}' and"
      }
      if(!queryScanner.objects.get(i).toString().toLowerCase().contains("?o")){
        tempStr += s" ${TripleOps.OBJECT_TABLE}.name='${queryScanner.objects.get(i)}' and"
      }
      if(!queryScanner.predicates.get(i).toString().toLowerCase().contains("?p")){
        tempStr += s" ${TripleOps.PREDICATE_TABLE}.name='${queryScanner.predicates.get(i)}' and"
      }
    }
    tempStr=tempStr.reverse.replaceFirst("dna","").reverse
    if(tempStr.length>5) {s" where (${tempStr})" }
    else {""}

}

  def getDistinct():String = {
    if(queryScanner.isDistinctEnabled){

      var groupBy=""
      for (i <- 0 to queryScanner.varList.size() - 1) {
        if (queryScanner.subjects.contains(queryScanner.varList.get(i))) {
          groupBy += s"${TripleOps.SUBJECT_TABLE}.name, "
        }
        else if (queryScanner.objects.contains(queryScanner.varList.get(i))) {
          groupBy += s"${TripleOps.OBJECT_TABLE}.name, "

        } else if (queryScanner.predicates.contains(queryScanner.varList.get(i))) {
          groupBy += s"${TripleOps.PREDICATE_TABLE}.name, "
        }
      }
      "group by "+ groupBy.reverse.replaceFirst(",", "").reverse
    }
    else
    {
      ""
    }
  }

  def getFilterCondition(): String ={
    var strCondition=""

    for( i <- 0 to queryScanner.filters.size()-1)
    {
      strCondition+=FilterCondition.getHDTFilter(queryScanner.filters.get(i))
      println(" Condition Processed: "+strCondition)
    }

    strCondition=strCondition.reverse.replaceFirst("dna","").reverse
    if(strCondition.length>5) s"where ${strCondition}" else ""
  }

  def getQuery(queryStr:String) ={

    queryScanner.reset
    //val queryStr="SELECT ?resource WHERE { ?resource ?x ?age . FILTER (?age >= 24)}  "
    val query = QueryFactory.create(queryStr)
    val op = Algebra.compile(query)
    OpWalker.walk(op, queryScanner)
    val result=s"select ${getProjectionFields()}from ${TripleOps.HDT_TABLE} inner join ${TripleOps.SUBJECT_TABLE} on ${TripleOps.HDT_TABLE}.s=${TripleOps.SUBJECT_TABLE}.index" +
      s" inner join ${TripleOps.OBJECT_TABLE} on ${TripleOps.HDT_TABLE}.o=${TripleOps.OBJECT_TABLE}.index" +
      s" inner join ${TripleOps.PREDICATE_TABLE} on ${TripleOps.HDT_TABLE}.p=${TripleOps.PREDICATE_TABLE}.index" +
      s" ${getWhereCondition()} ${getFilterCondition()} ${getDistinct()}"
    result
  }

}
object TripleOpsQueryNew {

  def execute(spark:SparkSession,rdfTriple: RDD[org.apache.jena.graph.Triple] , query:String): Unit ={

    var queryops=new TripleOpsQueryNew()

    var df=spark.sql(queryops.getQuery(query))
    val count=rdfTriple.sparql(query).count()
    println(s"SparQL Query : ${query}")
    println("Spark SQL: "+queryops.getQuery(query))
    println("SparQL Query Count: "+ count)
    println(s"Spark SQL Count: ${df.count()}")

  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TripleOps").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val tripleFile="/home/david/SANSA-Examples/sansa-examples-spark/src/main/resources/small/bsbm/sample.nt"
    val lang = Lang.NTRIPLES
    val rdfTriple = spark.rdf(lang)(tripleFile);
    val hdtDF = TripleOps.getHDT(rdfTriple)

    var query="SELECT ?S ?O ?P WHERE { ?S <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual4> ?P .  }"

    query=
     """
       SELECT ?S ?O ?P WHERE { FILTER( STRSTARTS(?p, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1" )) . }
     """.stripMargin
   execute(spark,rdfTriple,query)

    query="SELECT DISTINCT ?S ?O ?P WHERE { ?S ?P ?O }"
    execute(spark,rdfTriple,query)

    query="PREFIX foo: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> PREFIX hoo: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> SELECT ?S ?O ?P WHERE { foo:ProductType2 ?P ?O .  } limit 10"
    execute(spark,rdfTriple,query)


   //query="prefix test: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/> SELECT ?S ?O ?P WHERE { test:Product92 ?P ?O . ?S <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1> ?O . }"
   //execute(spark,rdfTriple,query)

    query="SELECT ?S ?O ?P WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product92> ?P ?O . ?S <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1> ?O . }"
    execute(spark,rdfTriple,query)

  }

}