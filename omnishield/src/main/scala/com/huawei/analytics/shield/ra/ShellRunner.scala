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

package com.huawei.analytics.shield.ra

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

import java.io.{BufferedReader, IOException, InputStreamReader, InterruptedIOException}
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.atomic.AtomicBoolean

/**
 * ShellRunner
 */
class ShellRunner extends Logging {
  private val completed: AtomicBoolean = new AtomicBoolean(false)

  def runCommand(shellAbsolutePath: String, handleRes:BufferedReader => Unit ): Unit = {
    val commend: util.ArrayList[String] = new util.ArrayList[String]()
    commend.add(shellAbsolutePath)
    val builder: ProcessBuilder = new ProcessBuilder(commend)
    completed.set(false)
    builder.redirectErrorStream(false)
    val process: Process = builder.start
    val stdErrReader: BufferedReader = new BufferedReader(new InputStreamReader(process.getErrorStream, StandardCharsets.UTF_8))
    val inReader: BufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream, StandardCharsets.UTF_8))
    val stdErrMsg: StringBuffer = new StringBuffer
    val errThread: Thread = new Thread() {
      override def run(): Unit = {
        var line: String = stdErrReader.readLine
        while (line != null && !isInterrupted) {
          stdErrMsg.append(line).append(System.getProperty("line.separator"))
          line = stdErrReader.readLine
        }
      }
    }
    errThread.start()
    try {
      handleRes(inReader)
      var line: String = inReader.readLine
      while (line != null) {
        line = inReader.readLine
      }
      val exitCode: Int = process.waitFor
      joinThread(errThread)
      completed.set(true)
      if (exitCode != 0) {
        val e = new SparkException(s"err exitcode: $exitCode , msg: " + stdErrMsg.toString)
        logError(e.getMessage)
        throw e
      }
    } catch {
      case ie: InterruptedException =>
        val iie: InterruptedIOException = new InterruptedIOException(ie.toString)
        iie.initCause(ie)
        throw iie
    } finally {
      try inReader.close()
      catch {
        case _: IOException =>
          logError("Error while closing the input stream")
      }
      if (!completed.get) {
        errThread.interrupt()
        joinThread(errThread)
      }
      try stdErrReader.close()
      catch {
        case _: IOException =>
          logError("Error while closing the error stream")
      }
      process.destroy()
    }
  }

  private def joinThread(t: Thread): Unit = {
    while (t.isAlive) {
      try t.join()
      catch {
        case _: InterruptedException =>
          t.interrupt()
      }
    }
  }

  def defaultHandleResult(lines: BufferedReader): Unit = {
    val output: StringBuffer = new StringBuffer
    val buf: Array[Char] = new Array[Char](512)
    var nRead: Int = lines.read(buf, 0, buf.length)
    while (nRead >0) {
      output.append(buf, 0, nRead)
      nRead = lines.read(buf, 0, buf.length)
    }
  }
}