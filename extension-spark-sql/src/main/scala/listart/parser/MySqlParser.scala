package listart.parser

import listart.command.ShowVersionCommand
import listart.parser.MySqlBaseParser._
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseErrorListener, ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}

class MySqlParser(val delegate: ParserInterface) extends ParserInterface {
  private val builder = new MySqlAstBuilder

  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    builder.visit(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ => delegate.parsePlan(sqlText)
    }
  }

  // scalastyle:off line.size.limit

  /**
   * Fork from `org.apache.spark.sql.catalyst.parser.AbstractSqlParser#parse(java.lang.String, scala.Function1)`.
   *
   * @see https://github.com/apache/spark/blob/v2.4.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L81
   */
  // scalastyle:on
  protected def parse[T](command: String)(toResult: MySqlBaseParser => T): T = {
    val lexer = new MySqlBaseLexer(
      new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new MySqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position)
    }
  }

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier = delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)
}

class MySqlAstBuilder extends MySqlBaseBaseVisitor[AnyRef] {
  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
    ShowVersionCommand()
  }

  override def visitPassThrough(ctx: PassThroughContext): LogicalPlan = null
}

// scalastyle:off line.size.limit

/**
 * Fork from `org.apache.spark.sql.catalyst.parser.UpperCaseCharStream`.
 *
 * @see https://github.com/apache/spark/blob/v2.4.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L157
 */
// scalastyle:on
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume()

  override def getSourceName: String = wrapped.getSourceName

  override def index(): Int = wrapped.index

  override def mark(): Int = wrapped.mark

  override def release(marker: Int): Unit = wrapped.release(marker)

  override def seek(where: Int): Unit = wrapped.seek(where)

  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) {
      wrapped.getText(interval)
    } else {
      ""
    }
  }

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

// scalastyle:off line.size.limit

/**
 * Fork from `org.apache.spark.sql.catalyst.parser.PostProcessor`.
 *
 * @see https://github.com/apache/spark/blob/v2.4.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L248
 */
// scalastyle:on
case object PostProcessor extends MySqlBaseBaseListener {

}