package net.sansa_stack.examples.spark.hdt

import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.jena.graph._

object TripleOps {

  val OBJECT_TABLE = "objects_hdt"
  val SUBJECT_TABLE = "subjects_hdt"
  val PREDICATE_TABLE = "predicates_hdt"
  val HDT_TABLE = "hdt"

  private val spark: SparkSession = SparkSession.builder().getOrCreate()

  /**
   * Function returns the Schema of Indexed Triple Fact table.
   * @return StructType
   */
  def hdtSchema: StructType = {
    StructType(
      Seq(
        StructField(name = "s", dataType = StringType, nullable = false),
        StructField(name = "o", dataType = StringType, nullable = false),
        StructField(name = "p", dataType = StringType, nullable = false)))
  }

  /**
   * Function returns the Schema of Dictionary Dataframe.
   * @return Schema of Dictionary
   */
  def dictionarySchema: StructType = {
    StructType(
      Seq(
        StructField(name = "name", dataType = StringType, nullable = false),
        StructField(name = "index", dataType = LongType, nullable = false)))
  }

  /**
   * Function converts RDD[graph.Triple] to DataFrame [Subject,Object,Predicate] by extracting SOP  value from each record
   * @param triple: Input raw RDD[graph.Triple]
   * @return Returns DataFrame [Subject,Object,Predicate]
   */
  def asHDT(triple: RDD[graph.Triple]): DataFrame = {
    spark.createDataFrame(triple.map(t => Row(t.getSubject.toString, t.getObject.toString(), t.getPredicate.toString())), hdtSchema)
  }

  /**
   * Return Dataframe of Index + Subject by retrieving the unique subjects from RDD[Triple] and zip it with undex
   * @param triples RDD[Triple] conversion of input file
   * @return DataFrame Subject dictionary of [index,subject]
   */
  def getDistinctSubjectDictDF(triples: RDD[graph.Triple]): DataFrame = {
    spark.createDataFrame(triples.map(_.getSubject.toString()).distinct().zipWithIndex().map(t => Row(t._1, t._2)), dictionarySchema).cache()
  }

  /**
   * Return Dataframe of Index + Predicate by retrieving the unique predicate from RDD[Triple] and zip it with undex
   * @param triples RDD[Triple] conversion of input file
   * @return DataFrame Predicate dictionary of [index,Prediate]
   */
  def getDistinctPredicateDictDF(triples: RDD[Triple]): DataFrame = {
    spark.createDataFrame(triples.map(_.getPredicate.toString()).distinct().zipWithIndex().map(t => Row(t._1, t._2)), dictionarySchema).cache()
  }

  /**
   * Return Dataframe of Index + Object by retrieving the unique objects from RDD[Triple] and zip it with undex
   * @param triples RDD[Triple] conversion of input file
   * @return DataFrame Object dictionary of [index , object]
   */
  def getDistinctObjectDictDF(triples: RDD[Triple]): DataFrame = {
    spark.createDataFrame(triples.map(_.getObject.toString()).distinct().zipWithIndex().map(t => Row(t._1, t._2)), dictionarySchema).cache()
  }

  /**
   * This is key function of TripleOps that read RDF file and create Dictionaries and Index Table and register them as Spark In memory Table
   * @param input Input RDF File Path [Either One of the input is require]
   * @param compressedDir Input compressed-directory Path to read compressed data directly [Either One of the input is require]
   * @param registerAsTable If true, it register all the DF as Spark table
   * @return Returns the Tuple4 [IndexDataFrame,SubjectDictDataFrame,ObjectDictDataFrame,PredicateDictDataFrame]
   */
  def getHDT(triples: RDD[Triple]): DataFrame = {

    val hdtDF = asHDT(triples)

    val object_hdt = getDistinctObjectDictDF(triples).createOrReplaceTempView(OBJECT_TABLE)
    val predicate_hdt = getDistinctPredicateDictDF(triples).createOrReplaceTempView(PREDICATE_TABLE)
    val subjectHDT = getDistinctSubjectDictDF(triples).createOrReplaceTempView(SUBJECT_TABLE)

    hdtDF.createOrReplaceTempView("triples_hdt")

    val sqlQuery = s"""
        SELECT ${SUBJECT_TABLE}.index as s, ${PREDICATE_TABLE}.index as p, ${OBJECT_TABLE}.index as o
        FROM triples_hdt
             JOIN ${SUBJECT_TABLE} ON triples_hdt.s = ${SUBJECT_TABLE}.name
             JOIN ${OBJECT_TABLE} ON triples_hdt.o = ${OBJECT_TABLE}.name
             JOIN ${PREDICATE_TABLE} ON triples_hdt.p =${PREDICATE_TABLE}.name
        """

    // Creating Fact table from Subject,Predicate and Object index. Fact table contains unique ID of Subject/Object/Predicate
    val hdt = spark.sql(sqlQuery)
    hdt.createOrReplaceTempView(HDT_TABLE)

    hdt
  }

  /**
   * Read hdt data from disk.
   * @param input -- path to hdt data.
   * @retun DataFrame of hdt, subject, predicate, and object view.
   */
  def readHDTFromDisk(input: String): (DataFrame, DataFrame, DataFrame, DataFrame) = {

    val hdt = spark.read.schema(hdtSchema).csv(input + "/triples")
    hdt.createOrReplaceTempView("hdt")

    val subjectDF = spark.read.schema(dictionarySchema)
      .csv(input + "/subject")
    subjectDF.createOrReplaceTempView("subjects_hdt")

    val objectDF = spark.read.schema(dictionarySchema)
      .csv(input + "/object")
    objectDF.createOrReplaceTempView("objects_hdt")

    val predicateDF = spark.read.schema(dictionarySchema)
      .csv(input + "/predicate")
    predicateDF.createOrReplaceTempView("predicates_hdt")

    (hdt, subjectDF, objectDF, predicateDF)
  }

  /**
   * Function saves the Index and Dictionaries Dataframe into given location
   * @param output Path to be written
   * @param mode SaveMode of Write
   */
  def saveAsCSV(hdt: DataFrame, subjectDF: DataFrame, predicateDF: DataFrame, objectDF: DataFrame, output: String, mode: SaveMode): Unit = {
    hdt.write.mode(mode).csv(output + "/triples")
    subjectDF.write.mode(mode).csv(output + "/subject")
    objectDF.write.mode(mode).csv(output + "/object")
    predicateDF.write.mode(mode).csv(output + "/predicate")
  }
}
