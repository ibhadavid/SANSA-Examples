package net.sansa_stack.examples.spark.hdt

import org.apache.jena.query.{Query, QueryFactory}
import org.apache.jena.sparql.algebra.op.OpFilter
import org.apache.jena.sparql.algebra.{Algebra, OpAsQuery}
import org.apache.jena.sparql.core.Prologue
import views.html.helper.inputFile

class TripleOpsQueryNew {


}
object TripleOpsQueryNew{
  def main(args: Array[String]): Unit = {

    val queryStr = "SELECT ?S ?O ?P WHERE { <http://dbpedia.org/resource/Vivian_Woodward> ?P ?O ." +
      " ?S <http://dbpedia.org/ontology/birthDate> ?O . }"

    val query = QueryFactory.create(queryStr)
    val op = Algebra.compile(query)

    import org.apache.jena.sparql.algebra.OpWalker

    val tc = new QueryScanner()
    OpWalker.walk(op, tc)

   // val triples = tc.getTriples

    println("Filter: "+tc.whereCondition)
    println("opQuadPattern: " +tc.triples)
    println("Projection:  "+tc.varList)

  }
}
