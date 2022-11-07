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
import java.nio.file.Path
import optimus.tools.scalacplugins.entity.reporter._

import scala.reflect.internal.util.NoPosition
import scala.reflect.internal.util.Position
import scala.reflect.internal.util.SourceFile
import scala.tools.nsc.Global

class PluginData(private val global: Global) {

  type Sourced = { def source: SourceFile }

  object ClearableCompat {
    // Import Clearable in a 2.12/2.13 cross buildable way
    import scala.collection.mutable._
    import scala.collection.generic._
    type Clearable_ = Clearable
  }
  private object onCompileFinished extends ClearableCompat.Clearable_ {
    override def clear(): Unit =
      closeClassLoader()

    // Temporary workaround until scalac will be fixed to close plugin classloader - more details in OPTIMUS-22254.
    // TODO (OPTIMUS-24773): We don't need this logic at all if we enable the
    // compiler's classpath caching, since that auto-closes the loader after the build completes.
    private def closeClassLoader(): Unit = {
      // if the compiler's classpath caching is not enabled, manually close the classloader else it can be left open
      // which causes the file lock on the Jar to be held, preventing recompilation of the plugin
      val noCache = global.settings.CachePolicy.None
      if (
        global.settings.YcacheMacroClassLoader.value == noCache && global.settings.YcachePluginClassLoader.value == noCache
      ) {
        import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader
        getClass.getClassLoader match {
          case cl: URLClassLoader =>
            try cl.close()
            catch { case e: Exception => reportException(e) }
          case _ => ()
        }
      }
    }

    private def reportException(e: Exception): Unit = {
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      e.printStackTrace(pw)
      println(sw)
      global.reporter.echo(NoPosition, sw.toString)
    }

  }

  def configureBasic(): Unit = {
    global.perRunCaches.recordCache(onCompileFinished)
    // always install the threshold profiler since it's lightweight and we always want to know about very slow compilation
    val thresholdProfiler = new ThresholdProfiler(global.reporter.echo,
      thresholdNs = slowCompilationWarningThresholdMs * 1000000)
    // keep the existing profiler (if any) so that we don't prevent -Yprofile from working
    global.currentRun.profiler = new DelegatingProfiler(Seq(global.currentRun.profiler, thresholdProfiler))
  }

  // TODO (OPTIMUS-51339): Deprecate because all this logic is moving to OBT
  class AlarmLevelConfiguration(val ignore: Set[Int], val silence: Set[Int], val debug: Boolean) {
    private def checkMandatory(id: Int): Unit = {
      OptimusAlarms.get(id) match {
        case None =>
          sys.error(s"Unknown alarm id $id")
        case Some(alarm) if alarm.obtMandatory && alarm.id.tpe != OptimusAlarmType.INFO =>
          sys.error(s"Cannot ignore mandatory alarm id $id")
        case Some(_: OptimusAlarmBuilder) => // ok
      }
    }

    ignore.foreach(checkMandatory)
    silence.foreach(checkMandatory)

    def getConfiguredLevel(sn: Int, declaredLevel: OptimusAlarmType.Tpe): OptimusAlarmType.Tpe = {
      // DEBUG level messages are always either lifted to INFO or downgraded to IGNORE depending
      // on whether -entity:debug flag .
      if (declaredLevel == OptimusAlarmType.DEBUG) {
        if (debug)
          OptimusAlarmType.INFO
        else
          OptimusAlarmType.SILENT
      } else if (silence.contains(sn)) OptimusAlarmType.SILENT
      else if (ignore.contains(sn) || OptimusAlarms.preIgnored.contains(sn)) OptimusAlarmType.INFO
      else declaredLevel
    }
  }

  // TODO (OPTIMUS-51339): Deprecate because all this logic is moving to OBT
  // all the values are be set by compiler plugin -P parameter
  object alarmConfig {
    var obtWarnConf = false // if true, use the OBT controlled warning process
    var debug = false
    // used to control the warning/error reporting level
    var ignore = Set.empty[Int]
    var silence = Set.empty[Int]

    def getConfiguredLevelRaw(
        alarmId: Int,
        alarmString: String,
        alarmLevel: String,
        positionSource: String,
        positionLine: Int,
        positionCol: Int,
        template: String): String =
      config.getConfiguredLevel(alarmId, OptimusAlarmType.withName(alarmLevel)).toString

    def getConfiguredLevel(alarmId: Int, level: OptimusAlarmType.Tpe): OptimusAlarmType.Tpe =
      config.getConfiguredLevel(alarmId, level)

    private lazy val config: AlarmLevelConfiguration = {
      new AlarmLevelConfiguration(alarmConfig.ignore, alarmConfig.silence, alarmConfig.debug)
    }
  }
  object rewriteConfig {
    var rewriteCollectionSeq: Boolean = false
    var rewriteMapValues: Boolean = false
    var rewriteBreakOutOps = false
    var rewriteAsyncBreakOutOps = false
    var rewriteToConversion = false
    var rewriteVarargsToSeq = false
    var rewriteMapConcatWiden = false
    var rewriteNilaryInfix = false
    var rewritePostfix = false
    var unitCompanion = false
    var procedureSyntax = false
    var autoApplication = false
    var nilaryOverride = false
    var anyFormatted = false
    var any2StringAdd = false
    var importShadow = false
    var rewriteCaseClassToFinal = false
    def anyEnabled =
      rewriteCollectionSeq || rewriteMapValues || rewriteBreakOutOps || rewriteAsyncBreakOutOps || rewriteToConversion || rewriteVarargsToSeq || rewriteMapConcatWiden || rewriteNilaryInfix || rewritePostfix || unitCompanion || procedureSyntax || autoApplication || nilaryOverride || anyFormatted || any2StringAdd || importShadow || rewriteCaseClassToFinal

    // disabled in some unit tests
    var useOptimusCompat: Boolean = true
  }

  var slowCompilationWarningThresholdMs = 20 * 1000L
}

object PublishDefinition {
  // used to reflectively call from a different class loader
  type PublisherType = {
    def publishRaw(
        dirPath: Path,
        alarmType: String,
        alarmId: Int,
        alarmMessage: String,
        positionSource: String,
        positionLine: Int): Unit
  }
}
