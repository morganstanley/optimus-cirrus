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
package optimus.tools.scalacplugins.entity

import java.io.PrintWriter
import java.io.StringWriter

import scala.reflect.internal.util.NoPosition
import scala.tools.nsc.Global
import scala.tools.nsc.plugins.Plugin

object StagingSettings {
  object OptionNames {
    val ignoreName = "ignore:"
    val silenceName = "silence:"
    val debugName = "debug:"
    val rewriteCollectionSeqName = "rewriteCollectionSeq:"
    val rewriteMapValuesName = "rewriteMapValues:"
    val rewriteBreakOutOpsName = "rewriteBreakOutOps:"
    val rewriteAsyncBreakOutOpsName = "rewriteAsyncBreakOutOps:"
    val rewriteToConversionName = "rewriteToConversion:"
    val rewriteVarargsToSeqName = "rewriteVarargsToSeq:"
    val rewriteMapConcatWidenName = "rewriteMapConcatWiden:"
    val rewriteNilaryInfixName = "rewriteNilaryInfix:"
    val unitCompanionName = "unitCompanion:"
    val anyFormattedName = "anyFormatted:"
    val any2StringAddName = "any2StringAdd:"
    val rewriteCaseClassToFinalName = "rewriteCaseClassToFinal:"
    val slowCompilationWarningThresholdMsName = "slowCompilationWarningThresholdMs:"
    val unsortedName = "unsorted:"
  }
}
object StagingPlugin {
  def parseOption(option: String, name: String, default: Boolean): Boolean = option.substring(name.length) match {
    case "false" | "disable" | "0" | "off" => false // Would be hard not to guess this one
    case "true" | "enable" | "1" | "on"    => true
    case _                                 => default
  }

  def parseInt(option: String, name: String): Int = option.substring(name.length).toInt

  def parseReportConfig(option: String, name: String): Set[String] = {
    option.substring(name.length).split(";").toSet
  }
  def parseReportConfigInt(option: String, name: String): Set[Int] = {
    parseReportConfig(option, name).map(_.toInt)
  }

  def processCommonOption(option: String, pluginData: PluginData, error: String => Unit): Unit = {
    import StagingSettings.OptionNames._

    // TODO (OPTIMUS-51339): Remove ignore: and silence: when OBT has taken over warnings.

    if (option.startsWith(ignoreName)) pluginData.alarmConfig.ignore = parseReportConfigInt(option, ignoreName)
    else if (option.startsWith(silenceName)) pluginData.alarmConfig.silence = parseReportConfigInt(option, silenceName)
    else if (option.startsWith(debugName)) pluginData.alarmConfig.debug = parseOption(option, debugName, false)
    else if (option.startsWith(rewriteCollectionSeqName))
      pluginData.rewriteConfig.rewriteCollectionSeq = parseOption(option, rewriteCollectionSeqName, false)
    else if (option.startsWith(rewriteMapValuesName))
      pluginData.rewriteConfig.rewriteMapValues = parseOption(option, rewriteMapValuesName, false)
    else if (option.startsWith(rewriteBreakOutOpsName))
      pluginData.rewriteConfig.rewriteBreakOutOps = parseOption(option, rewriteBreakOutOpsName, false)
    else if (option.startsWith(rewriteAsyncBreakOutOpsName))
      pluginData.rewriteConfig.rewriteAsyncBreakOutOps = parseOption(option, rewriteAsyncBreakOutOpsName, false)
    else if (option.startsWith(rewriteToConversionName))
      pluginData.rewriteConfig.rewriteToConversion = parseOption(option, rewriteToConversionName, false)
    else if (option.startsWith(rewriteVarargsToSeqName))
      pluginData.rewriteConfig.rewriteVarargsToSeq = parseOption(option, rewriteVarargsToSeqName, false)
    else if (option.startsWith(rewriteMapConcatWidenName))
      pluginData.rewriteConfig.rewriteMapConcatWiden = parseOption(option, rewriteMapConcatWidenName, false)
    else if (option.startsWith(rewriteNilaryInfixName))
      pluginData.rewriteConfig.rewriteNilaryInfix = parseOption(option, rewriteNilaryInfixName, false)
    else if (option.startsWith(unitCompanionName))
      pluginData.rewriteConfig.unitCompanion = parseOption(option, unitCompanionName, false)
    else if (option.startsWith(anyFormattedName))
      pluginData.rewriteConfig.anyFormatted = parseOption(option, anyFormattedName, false)
    else if (option.startsWith(any2StringAddName))
      pluginData.rewriteConfig.any2StringAdd = parseOption(option, any2StringAddName, false)
    else if (option.startsWith(rewriteCaseClassToFinalName))
      pluginData.rewriteConfig.rewriteCaseClassToFinal = parseOption(option, rewriteCaseClassToFinalName, false)
    else if (option.startsWith(slowCompilationWarningThresholdMsName))
      pluginData.slowCompilationWarningThresholdMs = parseInt(option, slowCompilationWarningThresholdMsName)
    else if (option.startsWith(unsortedName))
      pluginData.rewriteConfig.unsorted = parseOption(option, unsortedName, false)
    else error(s"unknown option '$option'")
  }

}

class StagingPlugin(val global: Global) extends Plugin {
  val name = "staging"
  val description = "Optimus platform staging plugin"

  val pluginData = new PluginData(global)
  override def init(options: List[String], error: String => Unit) = {
    try {
      for (option <- options) {
        StagingPlugin.processCommonOption(option, pluginData, error)
      }

      pluginData.configureBasic()
    } catch {
      case e: Exception =>
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        e.printStackTrace(pw)
        println(sw)
        global.reporter.echo(NoPosition, sw.toString)
        throw e
    }
    true
  }

  override val components = List(
    new StagingComponent(pluginData, global, StagingPhase.STAGING), // Before optimus_adjustast in entityplugin
    new CodingStandardsComponent(pluginData, global, StagingPhase.STANDARDS),
    new AnnotatingComponent(pluginData, global, StagingPhase.ANNOTATING),
    new PostTyperCodingStandardsComponent(pluginData, global, StagingPhase.POST_TYPER_STANDARDS),
    new GeneralAPICheckComponent(pluginData, global, StagingPhase.GENERAL_API_CHECK),
    new rewrite.RewriteComponent(pluginData, global, StagingPhase.REWRITE)
  )
}
