package scr.main.scala.com.huawei.booskit.spark

object ShuffleJoinStrategy extends Strategy
  with PredicateHelper
  with JoinSelectionHelper
  with SQLConfHelper {

  private val columnarPreferShuffledHashJoin = ColumnarPluginConfig.getSessionConf.columnarPreferShuffledHashJoin

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, nonEquicond, left, right, hint)
      if columnarPreferShuffledHashJoin =>
      val enable = getBroadcastBuildSide(left, right, joinType, hint, true, conf).isEmpty &&
      !hintToSortMergeJoin(hint) &&
      getShuffleHashJoinBuildSide(left, right, joinType, hint, true, conf).isEmpty &&
      !hintToShuffleReplicateNL(hint) &&
      getBroadcastBuildSide(left, right, joinType, hint, false, conf).isEmpty
      if (enable) {
        var buildLeft = false
        var buildRight = false
        var joinCountLeft = 0
        var joinCountRight = 0
        left.foreach(x => {
          if (x.isInstanceOf[Join]) {
            joinCountLeft = joinCountLeft + 1
          }
        })
        right.foreach(x => {
          if (x.isInstanceOf[Join]) {
            joinCountRight = joinCountRight + 1
          }
        })
        if ((joinCountLeft > 0) && (joinCountRight == 0)) {
          buildLeft = true
        }
        if ((joinCountRight > 0) && (joinCountLeft == 0)) {
          buildRight = true
        }

        getBuildSide(
          canBuildShuffledHashJoinLeft(joinType) && buildLeft,
          canBuildShuffledHashJoinRight(joinType) && buildRight,
          left,
          right
        ).map {
          buildSide =>
            Seq(joins.ShuffledHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              nonEquicond,
              planLater(left),
              planLater(right)))
        }.getOrElse(Nil)
      } else {
        Nil
      }
    case _ => Nil
  }

  private def getBuildSide(canBuildLeft: Boolean,
                           canBuildRight: Boolean,
                           left: LogicalPlan,
                           right: LogicalPlan): Option[BuildSide] = {
    if (canBuildLeft && canBuildRight) {
      // returns the smaller side base on its estimated physical size, if we want to build the
      // both sides
      Some(getSmallerSide(left, right))
    } else if (canBuildLeft) {
      Some(BuildLeft)
    } else if (canBuildRight) {
      Some(BuildRight)
    } else {
      None
    }
  }
}
