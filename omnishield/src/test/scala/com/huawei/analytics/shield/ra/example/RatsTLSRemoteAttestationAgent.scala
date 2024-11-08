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

package rats

import com.huawei.analytics.shield.ra.RemoteAttestationAgent
import org.apache.spark.internal.Logging

import java.io._
import java.lang
import java.net.Socket
import java.util.Properties
import javax.net.ssl.SSLSocketFactory
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class RatsTLSRemoteAttestationAgent extends RemoteAttestationAgent with Logging {

  private var socket: Socket = null
  private var oos: ObjectOutputStream = null
  private var ois: ObjectInputStream = null
  private val timeOutThreshold: Int = 10 * 1000
  private val socketConf = System.getenv("HADOOP_HOME") + "/rats/socket.properties"

  // step 1 . check port and start rats tls server
  // step 2 . start agent socket client and connect to server
  // step 3 . agent send remote attestation request
  // step 4 . write for reply
  // step 5 . return remote attestation res(boolean)
  override def doRemoteAttestation(executorHostName: String, executorId: String): Boolean = {
    var process: lang.Process = null
    try {
      val port = checkPort(executorId)
      process = startRatsTLSServer(port)
      setSocketConnection()
      val req = new RatsTLSRequest
      req.setExecutorInfo(executorHostName, executorId)
      req.setupRatsTlsServerInfo(executorHostName, port.toString)
      sendRequest(req)
      val reply = writeForReply()
      if (reply != null) {
        val res = reply.asInstanceOf[RatsTLSRes]
        res.getRes
      } else {
        false
      }
    } catch {
      case e: Throwable =>
        logError("doRemoteAttestation err")
        e.printStackTrace()
        throw e
    } finally {
      if (process != null && process.isAlive) {
        process.destroy()
      }
      closeConnection()
    }
  }

  override def requestSecret(executorHostName: String, executorId: String, secretId: String): Unit = {
    try {
      setSocketConnection()
      val req = new SecretRequest
      req.setExecutorInfo(executorHostName, executorId)
      req.setSecretId(secretId)
      sendRequest(req)
      val reply = writeForReply()
      if (reply != null) {
        val res = reply.asInstanceOf[SecretRes]
        if (res.getAvailable()) {
          this.putSecret(secretId, res.SecretJsonStr)
        }
      }
    } catch {
      case e: Throwable =>
        logError("doRemoteAttestation err")
        e.printStackTrace()
        throw e
    } finally {
      closeConnection()
    }
  }

  private def setSocketConnection(): Unit = {
    val properties = new Properties()
    properties.load(new FileInputStream(new File(socketConf)))
    val socketServerIP = properties.getProperty("serverip")
    val socketPort = properties.getProperty("serverport").toInt
    val trustStorePath = properties.getProperty("truststorepath")
    val trustStorePwd = properties.getProperty("truststorepwd")

    System.setProperty("javax.net.ssl.trustStore", trustStorePath)
    System.setProperty("javax.net.ssl.trustStorePassword", trustStorePwd)
    val factory = SSLSocketFactory.getDefault.asInstanceOf[SSLSocketFactory]
    socket = factory.createSocket(socketServerIP, socketPort)
    oos = new ObjectOutputStream(socket.getOutputStream)
    ois = new ObjectInputStream(socket.getInputStream)
  }

  private def sendRequest(request: Any): Unit = {
    if (socket != null) {
      oos.writeObject(request)
    } else {
      throw new Exception("socket not init")
    }
  }

  private def writeForReply(): Any = {
    var reply: Any = null
    val fut = Future {
      reply = ois.readObject
    }
    Await.result(fut, timeOutThreshold second)
    reply
  }

  private def closeConnection(): Unit = {
    if (socket != null && !socket.isClosed) {
      socket.close()
    }
  }

  private def checkPort(executorId: String): Int = {
    val basePort = 1233 + executorId.toInt
    basePort
  }

  private def startRatsTLSServer(port: Int): lang.Process = {
    logInfo("RatsTLSServer running")
    val ratsTlsProcess = Runtime.getRuntime.exec(s"virtcca-server -p $port")
    logInfo(s"RatsTLSServer start on port : $port")
    ratsTlsProcess
  }

}