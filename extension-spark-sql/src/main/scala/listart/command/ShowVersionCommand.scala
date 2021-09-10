package listart.command

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Properties

case class ShowVersionCommand() extends RunnableCommand {
  override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val javaVersion = Properties.javaVersion
    val scalaVersion = Properties.versionString
    val sparkVersion = sparkSession.version
    val outputString = s"java:$javaVersion\nscala:$scalaVersion\nspark:$sparkVersion"
    Seq(Row(outputString))
  }
}
