/*
 * Copyright © 2017-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.spark.dynamic.dataframe;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.dataframe.SparkDataframeCompute;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * A {@link SparkCompute} that takes any scala code and executes it.
 */
@Plugin(type = SparkDataframeCompute.PLUGIN_TYPE)
@Name("ScalaSparkDataframeCompute")
@Description("Executes user-provided Spark code written in Scala that performs Dataset to Dataset transformation")
public class ScalaSparkDataframeCompute extends SparkDataframeCompute<Row, Row> {

  private final transient Config config;
  // A strong reference is needed to keep the compiled classes around
  @SuppressWarnings("FieldCanBeLocal")
  private transient ScalaSparkDataframeCodeExecutor codeExecutor;
  private transient boolean isRDD;

  public ScalaSparkDataframeCompute(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    try {
      if (!config.containsMacro("schema")) {
        stageConfigurer.setOutputSchema(
          config.getSchema() == null ? stageConfigurer.getInputSchema() : Schema.parseJson(config.getSchema())
        );
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema " + config.getSchema(), e);
    }

    if (!config.containsMacro("scalaCode") && !config.containsMacro("dependencies")
      && Boolean.TRUE.equals(config.getDeployCompile())) {
      codeExecutor = new ScalaSparkDataframeCodeExecutor(config.getScalaCode(), config.getDependencies(), "transform", false);
      codeExecutor.configure(stageConfigurer.getInputSchema());
    }
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    codeExecutor = new ScalaSparkDataframeCodeExecutor(config.getScalaCode(), config.getDependencies(), "transform", false);
    codeExecutor.initialize(context);
    isRDD = !codeExecutor.isDataFrame();
  }

  @Override
  public Dataset transform(SparkExecutionPluginContext context,
                                             Dataset input) throws Exception {
    Dataset result = codeExecutor.execute(context, input);
    return result;

//    if (isRDD) {
//      //noinspection unchecked
//      return ((RDD<StructuredRecord>) result).toJavaRDD();
//    }
//
//    // Convert the DataFrame back to RDD<StructureRecord>
//    Schema outputSchema = context.getOutputSchema();
//    if (outputSchema == null) {
//      // If there is no output schema configured, derive it from the DataFrame
//      // Otherwise, assume the DataFrame has the correct schema already
//      outputSchema = DataFrames.toSchema((DataType) invokeDataFrameMethod(result, "schema"));
//    }
//    //noinspection unchecked
//    return ((JavaRDD<Row>) invokeDataFrameMethod(result, "toJavaRDD")).map(new RowToRecord(outputSchema));
  }

  /**
   * Configuration object for the plugin
   */
  public static final class Config extends PluginConfig {

    @Description("Spark code in Scala defining how to transform RDD to RDD. " +
      "The code must implement a function " +
      "called 'transform', which has signature as either \n" +
      "  def transform(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord]\n" +
      "  or\n" +
      "  def transform(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : RDD[StructuredRecord]\n" +
      "For example:\n" +
      "'def transform(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord] = {\n" +
      "   rdd.filter(_.get(\"gender\") == null)\n" +
      " }'\n" +
      "will filter out incoming records that does not have the 'gender' field."
    )
    @Macro
    private final String scalaCode;

    @Description(
      "Extra dependencies for the Spark program. " +
        "It is a ',' separated list of URI for the location of dependency jars. " +
        "A path can be ended with an asterisk '*' as a wildcard, in which all files with extension '.jar' under the " +
        "parent path will be included."
    )
    @Macro
    @Nullable
    private final String dependencies;

    @Description("The schema of output objects. If no schema is given, it is assumed that the output schema is " +
      "the same as the input schema.")
    @Nullable
    @Macro
    private final String schema;

    @Description("Decide whether to perform code compilation at deployment time. It will be useful to turn it off " +
      "in cases when some library classes are only available at run time, but not at deployment time.")
    @Nullable
    private final Boolean deployCompile;

    public Config(String scalaCode, @Nullable String schema, @Nullable String dependencies,
                  @Nullable Boolean deployCompile) {
      this.scalaCode = scalaCode;
      this.schema = schema;
      this.dependencies = dependencies;
      this.deployCompile = deployCompile;
    }

    public String getScalaCode() {
      return scalaCode;
    }

    @Nullable
    public String getSchema() {
      return schema;
    }

    @Nullable
    public String getDependencies() {
      return dependencies;
    }

    @Nullable
    public Boolean getDeployCompile() {
      return deployCompile;
    }
  }

  /**
   * Function to map from {@link Row} to {@link StructuredRecord}.
   */
  public static final class RowToRecord implements Function<Row, StructuredRecord> {

    private final Schema schema;

    public RowToRecord(Schema schema) {
      this.schema = schema;
    }

    @Override
    public StructuredRecord call(Row row) {
      return DataFrames.fromRow(row, schema);
    }
  }

  private static <T> T invokeDataFrameMethod(Object dataFrame, String methodName) throws Exception {
    //noinspection unchecked
    return (T) dataFrame.getClass().getMethod(methodName).invoke(dataFrame);
  }
}
