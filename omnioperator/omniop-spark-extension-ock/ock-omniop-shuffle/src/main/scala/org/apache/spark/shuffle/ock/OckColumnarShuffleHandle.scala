/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.shuffle.ock

import com.huawei.ock.ucache.shuffle.datatype.{RSSNodeInfo, RSSRepInfo, RpcAddress}
import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle

class OckColumnarShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V],
    secureId: String,
    _appAttemptId: String
  )
  extends BaseShuffleHandle(shuffleId, dependency) {
  var secCode: String = secureId
  var driverRpc: RpcAddress = null
  var bagsInfo: Array[Byte] = null
  var repInfo: RSSRepInfo = null
  var nodeInfo: RSSNodeInfo = null

  def appAttemptId : String = _appAttemptId

  def getBagsInfo: Array[Byte] = bagsInfo

  def rssRepInfo: RSSRepInfo = repInfo

  def rssNodeInfo: RSSNodeInfo = nodeInfo

  def getDriverRpc: RpcAddress = driverRpc

  def setBagsInfo(bags: Array[Byte]): OckColumnarShuffleHandle[K, V] = {
    bagsInfo = bags
    this
  }

  def setRepInfo(rep: RSSRepInfo): OckColumnarShuffleHandle[K, V] = {
    repInfo = rep
    this
  }

  def setNodeInfo(node: RSSNodeInfo): OckColumnarShuffleHandle[K, V] = {
    nodeInfo = node
    this
  }

  def setDriverRpc(rpc: RpcAddress): OckColumnarShuffleHandle[K, V] = {
    driverRpc = rpc
    this
  }
}