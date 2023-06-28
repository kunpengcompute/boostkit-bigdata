package org.apache.spark.sql

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, RadixSortExec, SortExec, SparkPlan}
import org.apache.spark.sql.types.LongType

class TelecomTenExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {

    // OmniCache optimizer rules
    //    extensions.injectPostHocResolutionRule { (session: SparkSession) =>
    //      OmniCacheOptimizerRule(session)
    //    }
    extensions.injectColumnar((session: SparkSession) => TestColumnar(session))
  }
}

case class TestColumnar(session: SparkSession) extends ColumnarRule {

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    plan.transformUp {
      case s@SortExec(sortOrder, global, child, testSpillFrequency) =>
        if (sortOrder.length == 2 &&
            sortOrder.head.dataType == LongType &&
            sortOrder(1).dataType == LongType &&
            session.sqlContext.conf.enableRadixSort) {
          RadixSortExec(sortOrder, global, child, testSpillFrequency)
        } else {
          s
        }
      case s => s
    }
  }
}