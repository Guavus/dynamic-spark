# Spark Computation in Scala

Description
-----------
Spark Computation in Scala is a Guavus Enterprise Accelerator that executes the user-provided Spark code in Scala, which is responsible for applying a logic on the incoming RDD and output a transformed RDD, with full access to all Spark features.

Use Case
--------
Consider a scenario wherein you want to have a Spark IDE where you can write custom Spark code with complete control over the Spark library. For example, you may want to join the input RDD with another dataset and then select a subset of the join result using Spark SQL. This can be achieved by configuring the accelerator as explained in the following section.

Properties
----------
**scalaCode:** The Spark code in Scala defining how to transform RDD to RDD. 
The code must implement a function called ``transform`` whose signature should be one of:

    def transform(df: DataFrame) : DataFrame

    def transform(df: DataFrame, context: SparkExecutionPluginContext) : DataFrame
    
The input ``DataFrame`` has the same schema as the input schema to this stage, and the ``transform`` method
should return a ``DataFrame`` that has the same schema as the output schema set up for this stage.
Using ``SparkExecutionPluginContext``, you can access CDAP
entities such as Stream and Dataset as it provides access to the underlying ``SparkContext`` in use.
 
Operating on the lower level ``RDD`` is also possible by using one of the following forms of the ``transform`` method:

    def transform(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord]

    def transform(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : RDD[StructuredRecord]
   
For example:

    def transform(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : RDD[StructuredRecord] = {
      val outputSchema = context.getOutputSchema
      rdd
        .flatMap(_.get[String]("body").split("\\s+"))
        .map(s => (s, 1))
        .reduceByKey(_ + _)
        .map(t => StructuredRecord.builder(outputSchema).set("word", t._1).set("count", t._2).build)
    }
        
This will perform a word count on the input field ``'body'`` and produce records of two fields, ``'word'`` and ``'count'``.

The following imports are included automatically and are ready to be used:

      import co.cask.cdap.api.data.format._
      import co.cask.cdap.api.data.schema._;
      import co.cask.cdap.etl.api.batch._
      import org.apache.spark._
      import org.apache.spark.api.java._
      import org.apache.spark.rdd._
      import org.apache.spark.sql._
      import org.apache.spark.SparkContext._
      import scala.collection.JavaConversions._

**dependencies** Extra dependencies for the Spark program.
It is a ',' separated list of URI for the location of dependency jars.
A path can be ended with an asterisk '*' as a wildcard, in which all files with extension '.jar' under the
parent path will be included.

**schema** The schema of output objects. If no schema is given, it is assumed that the output
schema is the same as the input schema.

**deployCompile** Specify whether the code will get validated during pipeline creation time. Setting this to `false`
will skip the validation.
