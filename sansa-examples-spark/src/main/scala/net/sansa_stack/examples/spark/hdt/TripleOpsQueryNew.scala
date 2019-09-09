package net.sansa_stack.examples.spark.hdt

import java.io.{File, PrintWriter}
import java.util.Calendar

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.algebra.OpWalker
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.rdd.RDD
import net.sansa_stack.query.spark.query._
import java.io.File

import scala.io.Source

class TripleOpsQueryNew() {

  val queryScanner = new QueryScanner()

  def isCountEnabled(): Boolean = {
    var status=false
    for (i <- 0 to queryScanner.aggregatorList.size() - 1) {
     if( queryScanner.aggregatorList.get(i).getAggregator.getName.equalsIgnoreCase("COUNT") ){
       status=true;
     }
    }
    status
  }

  def getProjectionFields() = {
    var result = ""

    if(isCountEnabled){
      result=" count(*) "
    }else{
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
    }

    //remove extra comma at the end
    result.reverse.replaceFirst(",", "").reverse
  }

  def getWhereCondition(): String = {
    var tempStr = ""
    for(i <- 0 to queryScanner.whereCondition.size()-1)
    {
      if(!queryScanner.optional.get(i)){
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
    }
    tempStr=tempStr.reverse.replaceFirst("dna","").reverse

    if(queryScanner.optional.contains(true)){
      for(i <- 0 to queryScanner.whereCondition.size()-1)
      {
        if(queryScanner.optional.get(i)){
          tempStr+=" or ( "
          if(!queryScanner.subjects.get(i).toString().toLowerCase().contains("?s")){
            tempStr += s" ${TripleOps.SUBJECT_TABLE}.name='${queryScanner.subjects.get(i)}' and"
          }
          if(!queryScanner.objects.get(i).toString().toLowerCase().contains("?o")){
            tempStr += s" ${TripleOps.OBJECT_TABLE}.name='${queryScanner.objects.get(i)}' and"
          }
          if(!queryScanner.predicates.get(i).toString().toLowerCase().contains("?p")){
            tempStr += s" ${TripleOps.PREDICATE_TABLE}.name='${queryScanner.predicates.get(i)}' and"
          }
          tempStr=tempStr.reverse.replaceFirst("dna","").reverse
          tempStr+=" )"
        }
      }
    }


    if(tempStr.length>5) {s" where (${tempStr})" }
    else {" where 1=1 "}

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
    var logicalOp=""

    for( i <- 0 to queryScanner.filters.size()-1)
    {
      val cond=FilterCondition.getHDTFilter(queryScanner.filters.get(i)) ;
      if(cond.length>2)
      {
        strCondition+=cond + " and "
      }

      //println(" Condition Processed: "+strCondition)
    }

    strCondition=strCondition.reverse.replaceFirst("dna","").reverse
    println(strCondition)
    if(strCondition.length>5) s" ${strCondition}" else " 1=1"
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
      s" ${getWhereCondition()} and ${getFilterCondition()} ${getDistinct()}"
    println(result)
    result
  }

}
object TripleOpsQueryNew {

  def execute(spark:SparkSession,rdfTriple: RDD[org.apache.jena.graph.Triple] , query:String, resultDir:String): Unit ={

    var queryops=new TripleOpsQueryNew()

    val pw = new PrintWriter(new File(s"${resultDir}/result_${Math.random()}.csv" ))
    pw.write(s" ${query} |")
    pw.append(s" ${queryops.getQuery(query)} |")
    var start=System.currentTimeMillis()
    var dfCount=spark.sql(queryops.getQuery(query)).count()
    val diff=(System.currentTimeMillis()-start)/1000.0;
    pw.append(s"${dfCount} | ${diff} |")

    start=System.currentTimeMillis()
   // val count=rdfTriple.sparql(query).count()
    //pw.append(s"${count} |  ${(System.currentTimeMillis()-start)/1000.0} ")
    pw.append("\n")
    pw.close()
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TripleOps").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if(args.length<3){
      println( " Invalid Arguments. ")
      println("Usage:")
      println("TripleOpsQueryNew <tripleFilePath> <resultDir> <queryDir>")
    }
    val tripleFile=args(0)
    println(s"Triple File: ${tripleFile}")
    
    val resultDir=args(1)
    println(s"Result Dir: ${resultDir}")

    val queryDir=args(2)
    println(s"Query Dir: ${queryDir}")


    val lang = Lang.NTRIPLES
    val rdfTriple = spark.rdf(lang)(tripleFile);
    val hdtDF = TripleOps.getHDT(rdfTriple)

    val d = new File(queryDir)

    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(f=>{
        print("Reading ${f.getAbsolutePath}")
        println(s"Executing Query: ${Source.fromFile(f.getAbsolutePath).mkString} ")
        execute(spark,rdfTriple,Source.fromFile(f.getAbsolutePath).mkString,resultDir)
      })
    } else {
      println("Please provide the Query directory.")
      System.exit(1)
    }


    /*
     var query=""

     query="SELECT ?S ?O ?P  WHERE { ?S ?P ?O }"
     execute(spark,rdfTriple,query,resultDir)

     query="SELECT ?S ?O ?P WHERE { ?S <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?O .  }"
     execute(spark,rdfTriple,query,resultDir)

      query="SELECT (COUNT(*) AS ?A)  WHERE { ?S ?P ?O }"
      execute(spark,rdfTriple,query,resultDir)

     query="SELECT DISTINCT ?S ?O ?P WHERE { ?S ?P ?O }"
     execute(spark,rdfTriple,query,resultDir)


     query="PREFIX foo: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> PREFIX hoo: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> SELECT ?S ?O ?P WHERE { foo:ProductType2 ?P ?O .  }"
     execute(spark,rdfTriple,query,resultDir)


     query="prefix test: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> SELECT ?S ?O ?P WHERE { test:ProductFeature92 ?P ?O . ?S <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/StandardizationInstitution1> ?O . }"
     execute(spark,rdfTriple,query,resultDir)

    query="SELECT ?S ?O ?P WHERE { ?S ?P ?O . FILTER ( STRLEN(?S) >= 40 ) . }"
     execute(spark,rdfTriple,query,resultDir)

    query="""
         SELECT ?s ?p WHERE {
         ?s ?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> .
         OPTIONAL { ?s <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/StandardizationInstitution1> ?o }
        }
      """.stripMargin
    execute(spark,rdfTriple,query,resultDir)
  */
}

}