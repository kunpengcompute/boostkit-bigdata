/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.analytics.shield.ra.example

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.{File, ObjectInputStream, ObjectOutputStream}
import javax.net.ssl.{SSLServerSocket, SSLServerSocketFactory}
import scala.collection.mutable

object RatsTLSRemoteAttestationServer {

  private var secretJsonFile:String = "secret.json"
  private var RatsTLSBaseInfoFile:String = "base.json"
  private var keyMap: mutable.HashMap[String, String] = mutable.HashMap[String, String]()


  def main(args: Array[String]): Unit = {
    importSecretFromLocalFile(secretJsonFile)
    val sslServerSocket = createSSLServerSocket(args(0))
    System.out.println("RatsTLSRemoteAttestationServer running")
    while (true) try {
      val socket = sslServerSocket.accept
      System.out.println("accept!\n")
      val ois = new ObjectInputStream(socket.getInputStream)
      val oos = new ObjectOutputStream(socket.getOutputStream)
      val obj = ois.readObject
      obj match {
        case rats: RatsTLSRequest => handleRatsTLSRequest(rats, oos)
        case secret: SecretRequest => handleSecretRequest(secret, oos)
      }
      socket.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  private def createSSLServerSocket(ksPath: String) = {
    System.setProperty("javax.net.ssl.keyStore", ksPath)
    System.setProperty("javax.net.ssl.keyStorePassword", "123456")
    val factory = SSLServerSocketFactory.getDefault
    val serverSocket = factory.createServerSocket(8443).asInstanceOf[SSLServerSocket]
    serverSocket
  }

  private def handleRatsTLSRequest(rats: RatsTLSRequest, oos: ObjectOutputStream): Unit = {
    println(s"remote attestation request arrived, executorHostName: ${rats.executorName} executorId: ${rats.executorId}, start handle msg...")
    val baseInfo = readRatsTLSBaseInfoFromLocalFile(rats.executorName)
    val exitCode = startRatsTlsClient(baseInfo)
    val reply = createReplyMsg(exitCode)
    System.out.println("start reply msg")
    oos.writeObject(reply)
    System.out.println("msg sending completed\n")
  }


  private def handleSecretRequest(secretReq: SecretRequest, oos: ObjectOutputStream): Unit = {
    println(s"SecretRequest arrived, executorHostName: ${secretReq.executorName} executorId: ${secretReq.executorId} secretId: ${secretReq.secretId}," +
      "start handle msg...\n")
    //Secrets can be obtained from kms or other components.
    //for demo, secrets from local files are used here.
    val secret = keyMap(secretReq.secretId)

    System.out.println("start reply msg")
    val res = new SecretRes()
    res.setSecret(secret)
    oos.writeObject(res)
    System.out.println("msg sending completed\n")
  }

  private def importSecretFromLocalFile(filePath:String) : Unit = {
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val jsonFile = new File(filePath)
    keyMap = mapper.readValue(jsonFile, classOf[mutable.HashMap[String, String]])
  }

  private def readRatsTLSBaseInfoFromLocalFile(hostName:String) : MeasurementInfo = {
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val jsonFile = new File(RatsTLSBaseInfoFile)
    try {
      val tempMap: Map[String, Map[String, String]] =
        mapper.readValue(jsonFile, classOf[Map[String, Map[String, String]]])
      val ipMap: Map[String, MeasurementInfo] = tempMap.map {
        case (ip, infoMap) => ip -> MeasurementInfo(infoMap("ip"), infoMap("baseValue"), infoMap("imaPath"))
      }
      ipMap(hostName)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  private def startRatsTlsClient(info:MeasurementInfo) : Int = {
    System.out.println(s"run  virtcca-client ip: ${info.ip} baseValue: ${info.baseValue} hashPath: ${info.imaHashPath}")
    val process:Process = Runtime.getRuntime.exec(s"virtcca-client -i ${info.ip} -r ${info.baseValue} -d ${info.imaHashPath}")
    val exitCode = process.waitFor
    System.out.println("startRatsTlsClient Exit Code: " + exitCode)
    exitCode
  }

  private def createReplyMsg(exitCode:Int) : RatsTLSRes = {
    val res = new RatsTLSRes()
    if(exitCode == 0){
      res.setRes(true)
      System.out.println("remote attestation pass")
    } else {
      res.setRes(false)
      System.out.println("remote attestation not pass")
    }
    res
  }
}

case class MeasurementInfo(ip: String, baseValue: String, imaHashPath:String)