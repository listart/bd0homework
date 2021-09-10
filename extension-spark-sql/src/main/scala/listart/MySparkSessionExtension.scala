package listart

import listart.parser.MySqlParser
import org.apache.spark.sql.SparkSessionExtensions

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      new MySqlParser(parser)
    }
    extensions.injectOptimizerRule { session =>
      MyPushDown(session)
    }
  }
}
