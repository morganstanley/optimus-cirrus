/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package optimus.buildtool.generators
import msjava.slf4jutils.scalalog.Logger
import optimus.buildtool.generators.JsonSchemaGenerator.JsonSchema2PojoSource.parseAnnotationStrConfig
import optimus.buildtool.generators.JsonSchemaGenerator.JsonSchema2PojoSource.parseSourceTypeStrConfig
import optimus.buildtool.generators.JsonSchemaGenerator._
import org.jsonschema2pojo.AnnotationStyle
import org.jsonschema2pojo.Annotator
import org.jsonschema2pojo.DefaultGenerationConfig
import org.jsonschema2pojo.InclusionLevel
import org.jsonschema2pojo.RuleLogger
import org.jsonschema2pojo.SourceSortOrder
import org.jsonschema2pojo.SourceType
import org.jsonschema2pojo.rules.RuleFactory

import java.io.File
import java.io.FileFilter
import java.net.URL

class JsonSchemaConfiguration(config: Map[String, String]) extends DefaultGenerationConfig {
  override def getSourceType(): SourceType =
    extract[SourceType](config, "sourceType", parseSourceTypeStrConfig, super.getSourceType)
  override def isGenerateBuilders: Boolean = extractBoolean(config, "generateBuilders", super.isGenerateBuilders)
  override def isIncludeTypeInfo: Boolean = extractBoolean(config, "includeTypeInfo", super.isIncludeTypeInfo)
  override def isUsePrimitives: Boolean = extractBoolean(config, "usePrimitives", super.isUsePrimitives)
  // there are no sources concept here so it is fine to delete this
  // we rely on 'files = [SomeSchema.json]' if source files need to be defined
  override def getSource: java.util.Iterator[URL] = super.getSource
  // this cannot be overriden as OBT generates the files into specific location
  override def getTargetDirectory: File = super.getTargetDirectory
  override def getTargetPackage: String = extractString(config, "targetPackage", super.getTargetPackage)
  override def getPropertyWordDelimiters: Array[Char] =
    config
      .get("propertyWordDelimiters")
      .map(_.replaceAll("[\\[\\]\\']", ""))
      .map(_.split(",").map(_.toCharArray.head))
      .getOrElse(super.getPropertyWordDelimiters)
  override def isUseLongIntegers: Boolean = extractBoolean(config, "useLongIntegers", super.isUseLongIntegers)
  override def isUseDoubleNumbers: Boolean = extractBoolean(config, "useDoubleNumbers", super.isUseDoubleNumbers)
  override def isIncludeHashcodeAndEquals: Boolean =
    extractBoolean(config, "includeHashcodeAndEquals", super.isIncludeHashcodeAndEquals)
  override def isIncludeToString: Boolean = extractBoolean(config, "includeToString", super.isIncludeToString)
  override def getToStringExcludes: Array[String] =
    config
      .get("toStringExcludes")
      .map(_.replaceAll("[\\[\\]]", "").split(",").distinct)
      .getOrElse(super.getToStringExcludes)
  override def isUseTitleAsClassname: Boolean =
    extractBoolean(config, "useTitleAsClassname", super.isUseTitleAsClassname)
  override def getAnnotationStyle: AnnotationStyle =
    extract[AnnotationStyle](config, "annotationStyle", parseAnnotationStrConfig, super.getAnnotationStyle)
  override def getInclusionLevel: InclusionLevel = super.getInclusionLevel
  override def getCustomAnnotator: Class[_ <: Annotator] =
    config
      .get("customAnnotator")
      .map(customAnnotator => Class.forName(customAnnotator).asInstanceOf[Class[Annotator]])
      .getOrElse(super.getCustomAnnotator)
  // use the default one
  override def getCustomRuleFactory: Class[_ <: RuleFactory] = super.getCustomRuleFactory
  override def isIncludeJsr303Annotations: Boolean =
    extractBoolean(config, "includeJsr303Annotations", super.isIncludeJsr303Annotations)
  override def isIncludeJsr305Annotations: Boolean =
    extractBoolean(config, "includeJsr305Annotations", super.isIncludeJsr305Annotations)
  override def isUseOptionalForGetters: Boolean =
    extractBoolean(config, "useOptionalForGetters", super.isIncludeToString)
  override def getOutputEncoding: String = extractString(config, "outputEncoding", super.getOutputEncoding)
  override def isRemoveOldOutput: Boolean = extractBoolean(config, "removeOldOutput", super.isRemoveOldOutput)
  override def isUseJodaDates: Boolean = extractBoolean(config, "useJodaDates", super.isUseJodaDates)
  override def isUseJodaLocalDates: Boolean = extractBoolean(config, "useJodaLocalDates", super.isUseJodaLocalDates)
  override def isUseJodaLocalTimes: Boolean = extractBoolean(config, "useJodaLocalTimes", super.isUseJodaLocalTimes)
  override def isParcelable: Boolean = extractBoolean(config, "parcelable", super.isParcelable)
  override def isSerializable: Boolean = extractBoolean(config, "serializable", super.isSerializable)
  // use the default one
  override def getFileFilter: FileFilter = super.getFileFilter
  override def isInitializeCollections: Boolean =
    extractBoolean(config, "initializeCollections", super.isInitializeCollections)
  override def getClassNamePrefix: String = extractString(config, "classNamePrefix", super.getClassNamePrefix)
  override def getClassNameSuffix: String = extractString(config, "classNameSuffix", super.getClassNameSuffix)
  override def getFileExtensions: Array[String] = config
    .get("fileExtensions")
    .map(_.replaceAll("[\\[\\]]", "").split(",").distinct)
    .getOrElse(super.getFileExtensions)
  override def isUseBigIntegers: Boolean = extractBoolean(config, "useBigIntegers", super.isUseBigIntegers)
  override def isUseBigDecimals: Boolean = extractBoolean(config, "useBigDecimals", super.isUseBigDecimals)
  override def isIncludeConstructors: Boolean =
    extractBoolean(config, "includeConstructors", super.isIncludeConstructors)
  override def isConstructorsRequiredPropertiesOnly: Boolean =
    extractBoolean(config, "constructorsRequiredPropertiesOnly", super.isConstructorsRequiredPropertiesOnly)
  override def isIncludeRequiredPropertiesConstructor: Boolean =
    extractBoolean(config, "includeRequiredPropertiesConstructor", super.isIncludeRequiredPropertiesConstructor)
  override def isIncludeAllPropertiesConstructor: Boolean =
    extractBoolean(config, "includeAllPropertiesConstructor", super.isIncludeAllPropertiesConstructor)
  override def isIncludeCopyConstructor: Boolean =
    extractBoolean(config, "includeCopyConstructor", super.isIncludeCopyConstructor)
  override def isIncludeAdditionalProperties: Boolean =
    extractBoolean(config, "includeAdditionalProperties", super.isIncludeAdditionalProperties)
  override def isIncludeGetters: Boolean = extractBoolean(config, "includeGetters", super.isIncludeGetters)
  override def isIncludeSetters: Boolean = extractBoolean(config, "includeSetters", super.isIncludeSetters)
  override def isIncludeDynamicAccessors: Boolean =
    extractBoolean(config, "includeDynamicAccessors", super.isIncludeDynamicAccessors)
  override def isIncludeDynamicGetters: Boolean =
    extractBoolean(config, "includeDynamicGetters", super.isIncludeDynamicGetters)
  override def isIncludeDynamicSetters: Boolean =
    extractBoolean(config, "includeDynamicSetters", super.isIncludeDynamicSetters)
  override def isIncludeDynamicBuilders: Boolean =
    extractBoolean(config, "includeDynamicBuilders", super.isIncludeDynamicBuilders)
  override def getDateTimeType: String = extractString(config, "dateTimeType", super.getDateTimeType)
  override def getDateType: String = extractString(config, "dateType", super.getDateType)
  override def getTimeType: String = extractString(config, "timeType", super.getTimeType)
  override def isFormatDateTimes: Boolean = extractBoolean(config, "formatDateTimes", super.isFormatDateTimes)
  override def isFormatDates: Boolean = extractBoolean(config, "formatDates", super.isFormatDates)
  override def isFormatTimes: Boolean = extractBoolean(config, "formatTimes", super.isFormatTimes)
  override def getRefFragmentPathDelimiters: String =
    extractString(config, "refFragmentPathDelimiters", super.getRefFragmentPathDelimiters)
  override def getCustomDatePattern: String = extractString(config, "customDatePattern", super.getCustomDatePattern)
  override def getCustomTimePattern: String = extractString(config, "customTimePattern", super.getCustomTimePattern)
  override def getCustomDateTimePattern: String =
    extractString(config, "customDateTimePattern", super.getCustomDateTimePattern)
  // use the default one
  override def getSourceSortOrder: SourceSortOrder = super.getSourceSortOrder
  // use the default one
  override def getFormatTypeMapping: java.util.Map[String, String] = super.getFormatTypeMapping
  override def isUseInnerClassBuilders: Boolean =
    extractBoolean(config, "useInnerClassBuilders", super.isUseInnerClassBuilders)
  override def isIncludeConstructorPropertiesAnnotation: Boolean =
    extractBoolean(config, "includeConstructorPropertiesAnnotation", super.isIncludeConstructorPropertiesAnnotation)
  override def isIncludeGeneratedAnnotation: Boolean =
    extractBoolean(config, "includeGeneratedAnnotation", super.isIncludeGeneratedAnnotation)
  // What Java version to target with generated source code (1.6, 1.8, 9, 11, etc).
  // By default, the version will be taken from the Gradle Java plugin's 'sourceCompatibility',
  // which (if unset) itself defaults to the current JVM version
  override def getTargetVersion: String = extractString(config, "targetVersion", super.getTargetVersion)
}

private[buildtool] class JsonSchema2PojoLogger(log: Logger) extends RuleLogger {
  override def debug(msg: String): Unit = log.debug(msg)
  override def error(msg: String): Unit = log.error(msg)
  override def error(msg: String, e: Throwable): Unit = if (e != null) log.error(msg, e) else log.error(msg)
  override def info(msg: String): Unit = log.info(msg)
  override def isDebugEnabled: Boolean = log.isDebugEnabled()
  override def isErrorEnabled: Boolean = log.isErrorEnabled()
  override def isInfoEnabled: Boolean = log.isInfoEnabled()
  override def isTraceEnabled: Boolean = log.isTraceEnabled()
  override def isWarnEnabled: Boolean = log.isWarnEnabled()
  override def trace(msg: String): Unit = log.trace(msg)
  override def warn(msg: String, e: Throwable): Unit = if (e != null) log.warn(msg, e) else log.warn(msg)
  override def warn(msg: String): Unit = log.warn(msg)
}
