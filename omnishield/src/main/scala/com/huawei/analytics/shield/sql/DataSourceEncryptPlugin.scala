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

package com.huawei.analytics.shield.sql

import com.huawei.analytics.shield.OmniContext
import com.huawei.analytics.shield.crypto.{AlgorithmMode, SM4_GCM_NOPADDING}
import com.huawei.analytics.shield.kms.common.KeyOperator
import com.huawei.analytics.shield.sql.SQLShieldOption.{COMPRESSION, CRYPTO_MODE, ENCRYPT_ENABLE, EN_DATA_KEY, KEY_LENGTH, KMS_TYPE, PKYE_NAME}
import com.huawei.analytics.shield.utils.LogError.invalidArgumentError
import com.huawei.analytics.shield.utils.ShieldConfParam.{WRITE_DATA_KEY_CIPHER_TEXT, genKmsTypeConf}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}

object SQLShieldOption {
  val PKYE_NAME: String = "keyname"
  val KEY_LENGTH: String = "keylength"
  val CRYPTO_MODE: String = "cryptomode"
  val KMS_TYPE: String = "kmstype"
  val EN_DATA_KEY: String = "encryptdatakey"
  val COMPRESSION: String = "compression"
  val ENCRYPT_ENABLE: String = "encrypt"
}

/**
 * DataSourceEncryptPlugin
 */
class DataSourceEncryptPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logInfo("Using BoostKit OmniShield DataSourceEncryptPlugin in Your SQL.")
    extensions.injectOptimizerRule(EncryptDataSourceRule)
  }
}

/**
 * EncryptDataSourceRule
 *
 * @param spark SparkSession
 */
case class EncryptDataSourceRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  var isApply: Boolean = false
  var isInsertSelect: Boolean = false
  var insertOptionMap: Option[Map[String, String]] = None
  var relationArray: Array[LogicalRelation] = Array()
  var createTbProperties: Option[Map[String, String]] = None
  var createTbType: Option[String] = None
  var insertTbType: Option[String] = None

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (isApply) {
      plan
    } else {
      isApply = true
      val hasTarget = findTargetPlan(plan)
      if (!hasTarget) {
        plan
      } else {
        val sc: OmniContext = OmniContext.loadOmniContext(spark)
        val resPlan = handleCreateTable(plan, sc)
        handleInsert(sc)
        handleScan(sc)
        resPlan
      }
    }
  }

  private def findTargetPlan(plan: LogicalPlan): Boolean = {
    val hasTarget = plan match {
      case createTable: CreateDataSourceTableCommand => {
        createTbProperties = Some(createTable.table.storage.properties)
        createTbType = createTable.table.provider
        true
      }
      case insertSelect: CreateDataSourceTableAsSelectCommand => {
        createTbProperties = Some(insertSelect.table.storage.properties)
        createTbType = insertSelect.table.provider
        isInsertSelect = true
        true
      }
      case insert: InsertIntoHadoopFsRelationCommand => {
        insertOptionMap = Some(insert.options)
        insertTbType = Some(insert.fileFormat match {
          case csv: CSVFileFormat => csv.shortName()
          case json: JsonFileFormat => json.shortName
          case text: TextFileFormat => text.shortName()
          case file => file.getClass.getSimpleName
        })
        true
      }
      case relation: LogicalRelation => {
        relationArray = relationArray :+ relation
        true
      }
      case _ => false
    }
    plan.children.foldLeft(hasTarget) {
      (last, next) => last | findTargetPlan(next)
    }
  }

  /**
   *  It gets the primaryKeyName, dataKeyLength, and kmsClassName parameters from the old properties in the
   *  createTable plan, then sets these parameters to keyOperator for generating encryptDateKey. Subsequently,
   *  it assigns encryptDataKey to the new properties and creates a new plan with these updated properties.
   *
   * @return plan with new properties
   */
  private def handleCreateTable(plan: LogicalPlan, sc: OmniContext): LogicalPlan = {
    if (createTbProperties.isEmpty || !createTbProperties.get.contains(ENCRYPT_ENABLE) ||
      !createTbProperties.get(ENCRYPT_ENABLE).toBoolean) {
      plan
    } else {
      parmaCheck(createTbProperties.get, createTbType.get)
      val keyOperator = buildKeyOperatorByMap(sc, createTbProperties.get)
      val encryptKeyBytes = keyOperator.getDataKeyCipherText
      val encryptKeyStr = keyOperator.encoderWithBase64(encryptKeyBytes)
      val newTbProperties = createTbProperties.get + (EN_DATA_KEY -> encryptKeyStr) +
        (COMPRESSION -> "com.huawei.analytics.shield.crypto.CryptoCodec")
      if (isInsertSelect) {
        val mode = createTbProperties.get(CRYPTO_MODE)
        val kms = createTbProperties.get(KMS_TYPE)
        setSQLWriteConfig(sc, mode, keyOperator, encryptKeyStr, kms)
      }
      replaceCreateCommandProperties(plan, newTbProperties)
    }
  }

  private def handleInsert(sc: OmniContext): Unit = {
    if (insertOptionMap.isDefined && insertOptionMap.get.contains(ENCRYPT_ENABLE) &&
      insertOptionMap.get(ENCRYPT_ENABLE).toBoolean) {
      parmaCheck(insertOptionMap.get, insertTbType.get)
      val mode = insertOptionMap.get(CRYPTO_MODE)
      val encryptDataKey = insertOptionMap.get(EN_DATA_KEY)
      val keyOperator = buildKeyOperatorByMap(sc, insertOptionMap.get)
      setSQLWriteConfig(sc, mode, keyOperator, encryptDataKey, insertOptionMap.get(KMS_TYPE))
    }
  }

  private def handleScan(sc: OmniContext): Unit = {
    if (relationArray.length > 0) {
      for (relation <- relationArray) {
        val tableProperties = relation.catalogTable.get.storage.properties
        val fileType = relation.catalogTable.get.provider.get
        if (tableProperties.contains(ENCRYPT_ENABLE) && tableProperties(ENCRYPT_ENABLE).toBoolean) {
          parmaCheck(tableProperties, fileType)
          val mode = tableProperties(CRYPTO_MODE)
          val encryptDataKey = tableProperties(EN_DATA_KEY)
          val keyOperator = buildKeyOperatorByMap(sc, tableProperties)
          setSQLReadConfig(sc, mode, keyOperator, encryptDataKey,tableProperties(KMS_TYPE))
        }
      }
    }
  }

  private def parmaCheck(values: Map[String, String], fileType: String): Unit = {
    invalidArgumentError(s"${PKYE_NAME} not found in options!",
      !values.contains(PKYE_NAME))
    invalidArgumentError(s"${KMS_TYPE} not found in options!",
      !values.contains(KMS_TYPE))
    invalidArgumentError(s"${KEY_LENGTH} not found in options!",
      !values.contains(KEY_LENGTH))
    invalidArgumentError(s"${CRYPTO_MODE} not found in options!",
      !values.contains(CRYPTO_MODE))
    val algorithm = AlgorithmMode.parse(values(CRYPTO_MODE))
    val keyLength = values(KEY_LENGTH).toInt
    algorithm match {
      case SM4_GCM_NOPADDING => invalidArgumentError(s"keyLength only 128" +
        s" on ${SM4_GCM_NOPADDING.encryptionAlgorithm} mode",
        keyLength != 128)
      case _ => invalidArgumentError("keyLength only 128 or 256",
        keyLength != 128 && keyLength != 256)
    }
    fileType match {
      case "csv" | "json" | "text" =>
      case t => invalidArgumentError(s"Unsupported  file type ${t}")
    }
  }

  private def buildKeyOperatorByMap(sc: OmniContext, values: Map[String, String]): KeyOperator = {
    val keyName = values(PKYE_NAME)
    val kms = values(KMS_TYPE)
    val keyLength = values(KEY_LENGTH)
    val algorithm = AlgorithmMode.parse(values(CRYPTO_MODE))
    sc.loadKeyOperator(keyName, kms, keyLength.toInt, algorithm)
  }

  private def setSQLWriteConfig(sc: OmniContext, mode: String, keyOperator: KeyOperator,
                                dataKeyCipherStr: String, kmsType: String): Unit = {
    sc.getHadoopConf.set(genKmsTypeConf(keyOperator.getPrimaryKeyName), kmsType)
    sc.getHadoopConf.set(WRITE_DATA_KEY_CIPHER_TEXT, dataKeyCipherStr)
    sc.setCommonConfig(mode, dataKeyCipherStr, keyOperator)
  }

  private def setSQLReadConfig(sc: OmniContext, mode: String, keyOperator: KeyOperator,
                       dataKeyCipherStr: String, kmsType: String): Unit = {
    sc.getHadoopConf.set(genKmsTypeConf(keyOperator.getPrimaryKeyName), kmsType)
    sc.setCommonConfig(mode, dataKeyCipherStr, keyOperator)
  }

  /**
   * createTable.table.storage.properties -> createTable.table.storage.newProperties
   *
   * @param plan plan
   * @return plan
   */
  private def replaceCreateCommandProperties(plan: LogicalPlan, newProperties: Map[String, String]): LogicalPlan = {
    plan match {
      case CreateDataSourceTableAsSelectCommand(
      CatalogTable(
      identifier,
      tableType,
      CatalogStorageFormat(
      locationUri,
      inputFormat,
      outputFormat,
      serde,
      compressed,
      _),
      schema,
      provider,
      partitionColumnNames,
      bucketSpec,
      owner,
      createTime,
      lastAccessTime,
      createVersion,
      properties,
      stats,
      viewText,
      comment,
      unsupportedFeatures,
      tracksPartitionsInCatalog,
      schemaPreservesCase,
      ignoredProperties,
      viewOriginalText),
      mode,
      query,
      outputColumnNames) => CreateDataSourceTableAsSelectCommand(CatalogTable(
        identifier,
        tableType,
        CatalogStorageFormat(
          locationUri,
          inputFormat,
          outputFormat,
          serde,
          compressed,
          newProperties
        ),
        schema,
        provider,
        partitionColumnNames,
        bucketSpec,
        owner, createTime,
        lastAccessTime,
        createVersion,
        properties,
        stats,
        viewText,
        comment,
        unsupportedFeatures,
        tracksPartitionsInCatalog,
        schemaPreservesCase,
        ignoredProperties,
        viewOriginalText), mode, query, outputColumnNames)
      case CreateDataSourceTableCommand(
      CatalogTable(
      identifier,
      tableType,
      CatalogStorageFormat(
      locationUri,
      inputFormat,
      outputFormat,
      serde,
      compressed,
      _),
      schema,
      provider,
      partitionColumnNames,
      bucketSpec,
      owner,
      createTime,
      lastAccessTime,
      createVersion,
      properties,
      stats,
      viewText,
      comment,
      unsupportedFeatures,
      tracksPartitionsInCatalog,
      schemaPreservesCase,
      ignoredProperties,
      viewOriginalText), ignoreIfExists) =>
        CreateDataSourceTableCommand(
          CatalogTable(
            identifier,
            tableType,
            CatalogStorageFormat(
              locationUri,
              inputFormat,
              outputFormat,
              serde,
              compressed,
              newProperties
            ),
            schema,
            provider,
            partitionColumnNames,
            bucketSpec,
            owner, createTime,
            lastAccessTime,
            createVersion,
            properties,
            stats,
            viewText,
            comment,
            unsupportedFeatures,
            tracksPartitionsInCatalog,
            schemaPreservesCase,
            ignoredProperties,
            viewOriginalText), ignoreIfExists)
      case p =>
        val children = plan.children.map(plan => replaceCreateCommandProperties(plan, newProperties))
        p.withNewChildren(children)
    }
  }
}