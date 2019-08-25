package net.sansa_stack.examples.spark.hdt

import java.util

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.algebra.OpVisitor
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.core.{Quad, Var}
import org.apache.jena.sparql.expr.{Expr, ExprAggregator}


/**
  * Created by xgfd on 01/03/2016.
  */
class QueryScanner extends OpVisitor {

  //  private val triples = new util.HashSet[_]

  //def getTriples: util.HashSet[_] = triples

  val whereCondition = new util.ArrayList[Triple]()
  val triples = new util.ArrayList[Quad]()
  var subjects=new util.ArrayList[Node]()
  var predicates=new util.ArrayList[Node]()
  var objects=new util.ArrayList[Node]()
  val varList = new util.ArrayList[Var]()
  val filters = new util.ArrayList[org.apache.jena.sparql.expr.Expr]()
  val aggregatorList = new util.ArrayList[ExprAggregator]()
  var isDistinctEnabled=false;
  var optional=new util.ArrayList[Boolean]()
  var indexOptional=0;

  def reset: Unit ={
    whereCondition.clear()
    triples.clear()
    subjects.clear()
    predicates.clear()
    objects.clear()
    varList.clear()
    filters.clear()
    isDistinctEnabled=false
  }

  override def visit(opBGP: OpBGP): Unit = {
    whereCondition.addAll(opBGP.getPattern.getList)
    //println(whereCondition)
    //println(optional)
    indexOptional+=1;
    for( i <- 0 to whereCondition.size()-1)
    {
      subjects.add(i,whereCondition.get(i).getSubject)
      objects.add(i,whereCondition.get(i).getObject)
      predicates.add(i,whereCondition.get(i).getPredicate)
      optional.add(false)


    }

  }

  override def visit(opQuadPattern: OpQuadPattern): Unit = {

    triples.addAll(opQuadPattern.getPattern.getList)
  }

  override def visit(opQuadBlock: OpQuadBlock): Unit = {
    println("opQuadBlock")
  }

  override def visit(opTriple: OpTriple): Unit = {
    println("opTriple")
  }

  override def visit(opQuad: OpQuad): Unit = {
    println("opQuad")
  }

  override def visit(opPath: OpPath): Unit = {
    println("opPath")
  }

  override def visit(opTable: OpTable): Unit = {
    println("opTable")
  }

  override def visit(opNull: OpNull): Unit = {
    println("opNull")
  }

  override def visit(opProcedure: OpProcedure): Unit = {
    println("opProcedure")
  }

  override def visit(opPropFunc: OpPropFunc): Unit = {
    println("opPropFunc")
  }

  override def visit(opFilter: OpFilter): Unit = {
    filters.addAll(opFilter.getExprs.getList)
    //println("Filter Condition: " + filters)
  }

  override def visit(opGraph: OpGraph): Unit = {
    println("opGraph")
  }

  override def visit(opService: OpService): Unit = {
    println("opService")
  }

  override def visit(opDatasetNames: OpDatasetNames): Unit = {
    println("opDatasetNames")
  }

  override def visit(opLabel: OpLabel): Unit = {
    println("opLabel")
  }

  override def visit(opAssign: OpAssign): Unit = {
    println("opAssign")
  }

  override def visit(opExtend: OpExtend): Unit = {
    println("opExtend")
  }

  override def visit(opJoin: OpJoin): Unit = {
    println("opJoin")
  }

  override def visit(opLeftJoin: OpLeftJoin): Unit = {
    //println("opLeftJoin")
    optional.set(indexOptional-1,true)
  }

  override def visit(opUnion: OpUnion): Unit = {
  }

  override def visit(opDiff: OpDiff): Unit = {
  }

  override def visit(opMinus: OpMinus): Unit = {
  }

  override def visit(opConditional: OpConditional): Unit = {
  }

  override def visit(opSequence: OpSequence): Unit = {
  }

  override def visit(opDisjunction: OpDisjunction): Unit = {
  }

  override def visit(opExt: OpExt): Unit = {
    println("opExt")
  }

  override def visit(opList: OpList): Unit = {
  }

  override def visit(opOrder: OpOrder): Unit = {
    //println("Order By: "+opOrder.getConditions)
  }

  override def visit(opProject: OpProject): Unit = {
    varList.addAll(opProject.getVars)
  }

  override def visit(opReduced: OpReduced): Unit = {
  }

  override def visit(opDistinct: OpDistinct): Unit = {
   // println("Distinct: "+ opDistinct.getName)
    if(opDistinct.getName.equals("distinct")){
      isDistinctEnabled=true
    }
  }

  override def visit(opSlice: OpSlice): Unit = {
  }


  override def visit(opGroup: OpGroup): Unit = {
    // println("Group: "+opGroup.getAggregators.get(0).getAggregator.getName)
    aggregatorList.addAll(opGroup.getAggregators)
  }

  override def visit(opTopN: OpTopN): Unit = {
    // println("Top : "+opTopN.getName)
  }
}