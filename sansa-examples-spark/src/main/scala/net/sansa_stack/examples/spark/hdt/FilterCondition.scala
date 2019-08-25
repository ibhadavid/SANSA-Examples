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
    //println("CreateFilterString: "+cond)
    //println("Expr Var: "+cond.getExprVar)
    //println("Constant : "+cond.getConstant)
    //println("Var Name: "+cond.getVarName)
    //println("getVarsMentioned : "+cond.getVarsMentioned)
    //println("As Var : "+cond.asVar())
    //println("As Var : ")

    var fName=cond.getFunction.getFunctionName(null);
    val argsList=cond.getFunction.getArgs


    if(fName.trim.equals("strstarts")){
      getColumnName(argsList.get(0).toString)+ s" like '${argsList.get(1).toString.replace("\"","")}%'"
    }
    else if(fName.toUpperCase.trim.equals("STRLEN")){
      //println("STRLEN");
      //println(argsList)
      s"length(${getColumnName(argsList.get(0).toString())})"

    }
    else if(fName.toUpperCase.trim.equals("SUBSTR")){
      //println(s"SUBSTR ${argsList.size()}");

      if(argsList.size()==2)
        s" substr(${getColumnName(argsList.get(0).toString)},${argsList.get(1).toString})"
      else if(argsList.size()==3)
        s" substr(${getColumnName(argsList.get(0).toString)},${argsList.get(1).toString},${argsList.get(2).toString})"
      else
        ""
    }
    else if(fName.toUpperCase.trim.equals("STRENDS")){
      //println("STRENDS");
      s"${getColumnName(argsList.get(0).toString)} like '%${argsList.get(1).toString.replace("\"","")}'"
      ""
    }
    else if(fName.toUpperCase.trim.equals("CONTAINS")){
      //println("CONTAINS");
      //println(argsList)
      s"${getColumnName(argsList.get(0).toString)} like '%${argsList.get(1).toString.replace("\"","")}%'"
    }
    else if(fName.toUpperCase.trim.equals("RAND")){
      //println("RAND");
      //println(argsList)
      " rand() "
    }
    else if(fName.toUpperCase.trim.equals("IN")){
      //println("IN");
      //println(argsList)
      ""
    }
    else if(fName.toUpperCase.trim.equals("NOT IN")){
      //println("NOT IN");
      //println(argsList)
      ""
    }
    else if(fName.toUpperCase.trim.equals("STRBEFORE")){
      //println("STRBEFORE");
      //println(argsList)
      s" substr(${getColumnName(argsList.get(0).toString)},0, instr(${getColumnName(argsList.get(0).toString)},'${argsList.get(1).toString.replace("\"","")}')-1) "
    }
    else if(fName.toUpperCase.trim.equals("STRAFTER")){
      //println("STRAFTER");
      //println(argsList)
      s" substr(${getColumnName(argsList.get(0).toString)},instr(${getColumnName(argsList.get(0).toString)},'${argsList.get(1).toString.replace("\"","")}') + length('${argsList.get(1).toString.replace("\"","")}')) "
    }
    else if(fName.toUpperCase.trim.equals("REPLACE")){
      //println("REPLACE");
      //println(argsList)
      s" replace(${getColumnName(argsList.get(0).toString)},'${argsList.get(1).toString.replace("\"","")}','${argsList.get(2).toString.replace("\"","")}')"
    }
    else if(fName.toUpperCase.trim.equals("GE"))
    {
      //println("Args List: "+argsList)
      if(argsList.get(0).isFunction){
        createFilterString(argsList.get(0)) + " >= "+ argsList.get(1).toString
      }
      else{
        getColumnName(argsList.get(0).toString) + " >= "+ argsList.get(1).toString
      }
    }
    else if(fName.toUpperCase.trim.equals("GT"))
    {
     // //println("Args List: "+argsList)
      if(argsList.get(0).isFunction){
        createFilterString(argsList.get(0)) + " >= "+ argsList.get(1).toString
      }
      else{
        getColumnName(argsList.get(0).toString) + " >= "+ argsList.get(1).toString
      }
    }
    else if(fName.toUpperCase.trim.equals("LT"))
    {
      ////println("Args List: "+argsList)
      if(argsList.get(0).isFunction){
        createFilterString(argsList.get(0)) + " <= "+ argsList.get(1).toString
      }
      else{
        getColumnName(argsList.get(0).toString) + " <= "+ argsList.get(1).toString
      }
    }
    else if(fName.toUpperCase.trim.equals("EQ"))
    {
      ////println("Args List: "+argsList)
      if(argsList.get(0).isFunction){
        createFilterString(argsList.get(0)) + " =  "+ argsList.get(1).toString
      }
      else{
        getColumnName(argsList.get(0).toString) + " = "+ argsList.get(1).toString
      }
    }
    else if(fName.trim.toUpperCase.equals("AND")){
      createFilterString(argsList.get(0)) + " and " +  createFilterString(argsList.get(1))
    }
    else if(fName.trim.toLowerCase().equals("OR")){
      createFilterString(argsList.get(0)) + " or " +  createFilterString(argsList.get(1))
    }
    else
    {
      throw new UnsupportedOperationException(s"Function not implemented ${fName}")
        ""
    }

  }
}
