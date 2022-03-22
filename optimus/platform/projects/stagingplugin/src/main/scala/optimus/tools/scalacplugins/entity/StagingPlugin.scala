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
    val rewriteBreakOutArgsName = "rewriteBreakOutArgs:"
    val rewriteVarargsToSeqName = "rewriteVarargsToSeq:"
    val rewriteMapConcatWidenName = "rewriteMapConcatWiden:"
    val rewriteNilaryInfixName = "rewriteNilaryInfix:"
    val unitCompanionName = "unitCompanion:"
    val anyFormattedName = "anyFormatted:"
  }
}
object StagingPlugin {
  def parseOption(option: String, name: String, default: Boolean): Boolean = option.substring(name.length) match {
    case "false" | "disable" | "0" | "off" => false // Would be hard not to guess this one
    case "true" | "enable" | "1" | "on"    => true
    case _                                 => default
  }

  def parseReportConfig(option: String, name: String): Set[String] = {
    option.substring(name.length).split(";").toSet
  }
  def parseReportConfigInt(option: String, name: String): Set[Int] = {
    parseReportConfig(option, name).map(_.toInt)
  }

  def processCommonOption(option: String, pluginData: PluginData, error: String => Unit): Unit = {
    import StagingSettings.OptionNames._

    if (option.startsWith(ignoreName)) pluginData.alarmConfig.ignore = parseReportConfigInt(option, ignoreName)
    else if (option.startsWith(silenceName)) pluginData.alarmConfig.silence = parseReportConfigInt(option, silenceName)
    else if (option.startsWith(debugName)) pluginData.alarmConfig.debug = parseOption(option, debugName, false)
    else if (option.startsWith(rewriteCollectionSeqName))
      pluginData.rewriteConfig.rewriteCollectionSeq = parseOption(option, rewriteCollectionSeqName, false)
    else if (option.startsWith(rewriteMapValuesName))
      pluginData.rewriteConfig.rewriteMapValues = parseOption(option, rewriteMapValuesName, false)
    else if (option.startsWith(rewriteBreakOutOpsName))
      pluginData.rewriteConfig.rewriteBreakOutOps = parseOption(option, rewriteBreakOutOpsName, false)
    else if (option.startsWith(rewriteBreakOutArgsName))
      pluginData.rewriteConfig.rewriteBreakOutArgs = parseOption(option, rewriteBreakOutArgsName, false)
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
    new ForwardingComponent(pluginData, global, StagingPhase.FORWARDING),
    new rewrite.RewriteComponent(pluginData, global, StagingPhase.REWRITE)
  )
}
