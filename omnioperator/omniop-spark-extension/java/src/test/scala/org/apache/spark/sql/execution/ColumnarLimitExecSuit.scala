package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

class ColumnarLimitExecSuit extends ColumnarSparkPlanTest {

  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var left: DataFrame = _
  private var right: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    left = Seq[(java.lang.Integer, java.lang.Integer, java.lang.Integer)](
      (1, 1, 1),
      (2, 2, 2),
      (3, 3, 3),
      (4, 5, 6)
    ).toDF("a", "b", "c")

    right = Seq[(java.lang.Integer, java.lang.Integer, java.lang.Integer)](
      (1, 1, 1),
      (2, 2, 2),
      (3, 3, 3)
    ).toDF("x", "y", "z")
  }

  test("Push down limit through LEFT SEMI and LEFT ANTI join") {
    withTable("left_table", "nonempty_right_table", "empty_right_table") {
      spark.sql("SET spark.sql.adaptive.enable=false")
      spark.range(5).toDF().repartition(1).write.saveAsTable("left_table")
      spark.range(3).write.saveAsTable("nonempty_right_table")
      spark.range(0).write.saveAsTable("empty_right_table")
      Seq("LEFT SEMI", "LEFT ANTI").foreach { joinType =>
        val joinWithNonEmptyRightDf = spark.sql(
          s"SELECT * FROM left_table $joinType JOIN nonempty_right_table LIMIT 3")
        val joinWithEmptyRightDf = spark.sql(
          s"SELECT * FROM left_table $joinType JOIN empty_right_table LIMIT 3")

        val expectedAnswer = Seq(Row(0), Row(1), Row(2))
        if (joinType == "LEFT SEMI") {
          checkAnswer(joinWithNonEmptyRightDf, expectedAnswer)
          checkAnswer(joinWithEmptyRightDf, Seq.empty)
        } else {
          checkAnswer(joinWithNonEmptyRightDf, Seq.empty)
          checkAnswer(joinWithEmptyRightDf, expectedAnswer)
        }

        Seq(joinWithNonEmptyRightDf, joinWithEmptyRightDf).foreach { df =>
          val pushedLocalLimits = df.queryExecution.executedPlan.collect {
            case l : ColumnarLocalLimitExec => l
          }
          assert(pushedLocalLimits.length === 2)
        }
      }
    }
  }

  test("Push down limit through left join") {
    val res = left.join(right.hint("broadcast"), col("a") === col("x"), "leftouter").limit(3)
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarLocalLimitExec]).isDefined,
      s"ColumnarShuffledHashJoinExec not happened," +
        s" executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }
}
