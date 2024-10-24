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

import org.apache.hadoop.fs.FileUtil
import org.apache.spark.SparkFiles

import java.io.{File, FileOutputStream, PrintStream, UnsupportedEncodingException}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.mutable

/**
 * Generate a shell to perform the ima measurement of open euler OS，
 * The script measures 3 items: 1.Spark Application JAR 2.Spark Library JARs 3.Hadoop YARN JARs (if need)
 * After the measurement policy is configured in the open euler OS,
 * the ima measurement is automatically performed when the file is read.Spark Library JARs and Hadoop YARN JARs
 * are usually a file directory. We will run the "tar" command to package the file directory and read it.
 */
class ShellGeneration {
  private val shellSb = new StringBuilder
  private val LINE_SEPARATOR = System.getProperty("line.separator")

  protected def addToShell(command: String*): Unit = {
    for (s <- command) {
      shellSb.append(s)
    }
    shellSb.append(LINE_SEPARATOR)
  }

  def UnixShellScriptBuilder(): Unit = {
    addToShell("#!/bin/bash")
    addToShell()
  }

  def setExitOnFailure(): Unit = {
    addToShell("set -o pipefail -e")
  }

  def copyFile(sourcePath: String, targetPath: String): Unit = {
    addToShell(s"cp -r $sourcePath $targetPath")
  }

  def head(filePath: String): Unit = {
    addToShell(s"sudo head -n 1 $filePath >/dev/null 2>&1")
  }

  def echoErr(msg: String): Unit = {
    addToShell(s"echo $msg >&2")
  }

  def chown(ownerName: String, filePath: String): Unit = {
    addToShell(s"sudo chown -R $ownerName $filePath")
  }

  def packJarFiles(sPath: String, tPath:String): Unit = {
    addToShell(s"cd $sPath")
    addToShell(s"find . -name '*.jar' -print0 | sort -z | tar --null --owner=0 --group=0 " +
      s"--no-acls --no-same-permissions --mtime='1970-01-01 12:00:00 UTC' -cvf $tPath --files-from=-")
    addToShell(s"cd -")
  }

  def source(): Unit = {
    addToShell(s"source /etc/profile")
  }

  def exit(number: Int): Unit = {
    addToShell(s"exit $number")
  }

  def clearBuffer(): Unit = {
    shellSb.clear()
  }

  def genFile(shellName: String): String = {
    val targetFile = new File(SparkFiles.getRootDirectory(), shellName)
    var out: FileOutputStream = null
    var pout: PrintStream = null
    try {
      out = new FileOutputStream(targetFile)
      pout = new PrintStream(out, false, StandardCharsets.UTF_8.toString)
      pout.append(shellSb)
      FileUtil.chmod(targetFile.getAbsolutePath, "a+x")
      targetFile.getAbsolutePath
    } catch {
      case e: UnsupportedEncodingException =>
        throw e
    } finally {
      if (pout != null) pout.close()
      if (out != null) out.close()
    }
  }
}

object IMAMeasurementShellGeneration extends ShellGeneration {

  private val SPARK_LOCALIZED_LIB_DIR = "__spark_libs__"
  private val IMA_USER_ENV ="$IMA_USER"
  private val HADOOP_MEASUREMENT_DIR = "$HADOOP_MEASUREMENT_DIR"
  private val APP_LOCAL_DIR=System.getenv("LOCAL_DIRS")

  def genIMAShellContent(fileMap: mutable.Map[String, Long]): String = {
    clearBuffer()
    //put spark lib to
    fileMap.put(SPARK_LOCALIZED_LIB_DIR,1)

    UnixShellScriptBuilder()
    setExitOnFailure()
    source()
    //check ima user
    addToShell(s"if [ ! $IMA_USER_ENV ]")
    addToShell(s"then")
    echoErr(" ima user is null")
    exit(1)
    addToShell(s"fi")

    for ((name, _) <- fileMap) {
      val localName = new URI(name).getPath.split("/").last
      val file = new File(SparkFiles.getRootDirectory(), localName)
      if (file.exists() && file.isFile) {
        val path = file.toPath
        val truePath = if (Files.isSymbolicLink(path)) {
          Files.readSymbolicLink(path).toUri.getPath
        } else {
          file.getAbsolutePath
        }
        val imaFileName = s"${SparkFiles.getRootDirectory()}/$localName.ima"
        addToShell(s"if [ ! -e $imaFileName ]")
        addToShell(s"then")
        copyFile(truePath, imaFileName)
        chown(IMA_USER_ENV, imaFileName)
        head(imaFileName)
        addToShell(s"fi")
      }
      if (file.exists() && file.isDirectory) {
        val tarPath = s"/$APP_LOCAL_DIR/$localName.ima"
        addToShell(s"if [ ! -e $tarPath ]")
        addToShell(s"then")
        packJarFiles(file.getCanonicalFile.getPath, tarPath)
        chown(IMA_USER_ENV, tarPath)
        head(tarPath)
        addToShell(s"fi")
      }
    }

    //measurement hadoop if need
    val hadoopIMAFile = s"/$APP_LOCAL_DIR/hadoop.ima"
    addToShell(s"if [[ $HADOOP_MEASUREMENT_DIR && ! -e $hadoopIMAFile ]]")
    addToShell(s"then")
    packJarFiles(HADOOP_MEASUREMENT_DIR,  hadoopIMAFile)
    chown(IMA_USER_ENV, hadoopIMAFile)
    head(hadoopIMAFile)
    addToShell(s"fi")
    genFile("imaMeasurement.sh")
  }

}