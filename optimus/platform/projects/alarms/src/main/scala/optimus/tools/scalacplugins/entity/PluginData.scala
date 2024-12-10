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

import scala.reflect.internal.util.NoPosition
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
    /* val thresholdProfiler =
      new ThresholdProfiler(global.reporter.echo(_, _), thresholdNs = slowCompilationWarningThresholdMs * 1000000)
    // keep the existing profiler (if any) so that we don't prevent -Yprofile from working
    global.currentRun.profiler = new DelegatingProfiler(Seq(global.currentRun.profiler, thresholdProfiler)) */
  }

  // warns if obtWarnConf is set
  if (global.settings.defines.value.contains("-Doptimus.buildtool.warnings=false"))
    global.reporter.warning(NoPosition, "-Doptimus.buildtool.warnings=false is deprecated!")

  // all the values are be set by compiler plugin -P parameter
  object alarmConfig {
    var debug = false
    // used to control the warning/error reporting level
    var silence = Set.empty[Int]
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
    var intToFloat = false
    var unsorted = false
    def anyEnabled =
      rewriteCollectionSeq || rewriteMapValues || rewriteBreakOutOps || rewriteAsyncBreakOutOps || rewriteToConversion || rewriteVarargsToSeq || rewriteMapConcatWiden || rewriteNilaryInfix || rewritePostfix || unitCompanion || procedureSyntax || autoApplication || nilaryOverride || anyFormatted || any2StringAdd || importShadow || rewriteCaseClassToFinal || intToFloat || unsorted

    // disabled in some unit tests
    var useOptimusCompat: Boolean = true
  }

  // TODO (OPTIMUS-48502): Drop this once we drop 2.12 support and then adopt 2.13 meaning of Seq
  var varargs213Compat = true

  var slowCompilationWarningThresholdMs = 20 * 1000L
  var forceLoad: List[String] = Nil
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
