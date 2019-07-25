package net.sansa_stack.examples.spark.hdt

import org.apache.jena.sparql.expr.Expr

object FilterCondition {

  def getHDTFilter(cond:Expr): String ={
    createFilterString(cond)
  }

  def getColumnName(value: String) = {
    if(value.trim.equalsIgnoreCase("?S")){
      TripleOps.SUBJECT_TABLE+".name"
    }
    else if(value.trim.equalsIgnoreCase("?P")){
      TripleOps.PREDICATE_TABLE+".name"
    }
    else if(value.trim.equalsIgnoreCase("?O")){
      TripleOps.OBJECT_TABLE+".name"
    }
    else
    {
      ""
    }
  }

  def createFilterString(cond:Expr): String ={
    var fName=cond.getFunction.getFunctionName(null);
    val argsList=cond.getFunction.getArgs

    if(fName.trim.equals("strstarts")){
      getColumnName(argsList.get(0).toString)+ s" like '${argsList.get(1).toString.replace("\"","")}%'"
    }
    else
    {
        ""
    }

  }
}
