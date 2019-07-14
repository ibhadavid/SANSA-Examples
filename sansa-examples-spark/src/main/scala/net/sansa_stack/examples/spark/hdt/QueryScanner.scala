package net.sansa_stack.examples.spark.hdt

import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.OpVisitor
import org.apache.jena.sparql.algebra.op._
import java.util
import java.util.stream.Collectors

import org.apache.jena.sparql.core.{Quad, Var}


/**
  * Created by xgfd on 01/03/2016.
  */
class QueryScanner extends OpVisitor {

//  private val triples = new util.HashSet[_]

  //def getTriples: util.HashSet[_] = triples

  val whereCondition = new util.HashSet[Triple]()
  val triples = new util.HashSet[Quad]()
  val varList = new util.HashSet[Var]()

  override def visit(opBGP: OpBGP): Unit = {
    whereCondition.addAll(opBGP.getPattern.getList)
  }

  override def visit(opQuadPattern: OpQuadPattern): Unit = {

    triples.addAll(opQuadPattern.getPattern.getList)
  }

  override def visit(opQuadBlock: OpQuadBlock): Unit = {
  }

  override def visit(opTriple: OpTriple): Unit = {
  }

  override def visit(opQuad: OpQuad): Unit = {
  }

  override def visit(opPath: OpPath): Unit = {
  }

  override def visit(opTable: OpTable): Unit = {
  }

  override def visit(opNull: OpNull): Unit = {
  }

  override def visit(opProcedure: OpProcedure): Unit = {
  }

  override def visit(opPropFunc: OpPropFunc): Unit = {
  }

  override def visit(opFilter: OpFilter): Unit = {
  }

  override def visit(opGraph: OpGraph): Unit = {
  }

  override def visit(opService: OpService): Unit = {
  }

  override def visit(opDatasetNames: OpDatasetNames): Unit = {
  }

  override def visit(opLabel: OpLabel): Unit = {
  }

  override def visit(opAssign: OpAssign): Unit = {
  }

  override def visit(opExtend: OpExtend): Unit = {
  }

  override def visit(opJoin: OpJoin): Unit = {
  }

  override def visit(opLeftJoin: OpLeftJoin): Unit = {
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
  }

  override def visit(opList: OpList): Unit = {
  }

  override def visit(opOrder: OpOrder): Unit = {
  }

  override def visit(opProject: OpProject): Unit = {
    varList.addAll(opProject.getVars)
  }

  override def visit(opReduced: OpReduced): Unit = {
  }

  override def visit(opDistinct: OpDistinct): Unit = {
    println(opDistinct.getSubOp)
  }

  override def visit(opSlice: OpSlice): Unit = {
  }

  override def visit(opGroup: OpGroup): Unit = {
  }

  override def visit(opTopN: OpTopN): Unit = {
    println(opTopN.getName)
  }
}