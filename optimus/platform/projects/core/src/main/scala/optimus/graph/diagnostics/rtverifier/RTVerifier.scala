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
package optimus.graph.diagnostics.rtverifier

import msjava.slf4jutils.scalalog.getLogger
import optimus.core.MonitoringBreadcrumbs
import optimus.debug.InstrumentedModuleCtor
import optimus.debug.InstrumentedNotRTFunction
import optimus.debug.RTVerifierCategory
import optimus.graph.OGSchedulerContext
import optimus.graph.PropertyNodeSync
import optimus.graph.loom.AsNode
import optimus.platform.util.PrettyStringBuilder

import java.lang.StackWalker.StackFrame
import java.util.stream.{Stream => JStream}
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

object RTVerifier {
  private val log = getLogger(this)

  private val internalPackages = Set(
    "optimus.debug",
    "optimus.graph.diagnostics.rtverifier",
    "optimus.graph",
    "optimus.graph.loom",
    "optimus.graph.profiled"
  )

  def register(): Unit = {
    def runSafely(f: => Unit): Unit = Try(f).recover { case NonFatal(ex) =>
      MonitoringBreadcrumbs.sendRTVerifierTriggerFailure(ex)
      log.error("RT Verifier Trigger Failure", ex)
    }
    InstrumentedModuleCtor.callback = { _ => runSafely(mCtorCallback()) }
    InstrumentedNotRTFunction.callback = () => runSafely(reportIfTransitivelyCached())
  }

  private def mCtorCallback(): Unit = {
    // StackWalker is thread-safe so using mutable collections and vars is ok
    StackWalker
      .getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
      .walk[Array[AnyRef]]((stream: JStream[StackFrame]) => {
        val visitedFrames = ArrayBuffer.empty[StackTraceElement]
        var firstFrame: StackTraceElement = null
        var lastFrame: StackTraceElement = null
        var category = RTVerifierCategory.NONE
        val interestingFrames = stream
          .map[StackFrame] { frame =>
            visitedFrames.append(frame.toStackTraceElement)
            frame
          }
          .dropWhile { frame =>
            val isTrigger =
              frame.getDeclaringClass == classOf[InstrumentedModuleCtor] && frame.getMethodName == "trigger"
            !isTrigger
          }
          .skip(1) // dropping the InstrumentedModuleCtor.trigger frame
          .dropWhile { frame =>
            val toDrop = internalPackages.contains(frame.getDeclaringClass.getPackageName)
            if (toDrop) firstFrame = frame.toStackTraceElement
            toDrop
          }
          .takeWhile { frame =>
            lastFrame = frame.toStackTraceElement
            category = isFrameOfInterest(frame)
            category == RTVerifierCategory.NONE
          }
          .map[StackTraceElement](_.toStackTraceElement)
          .toArray
        val frames = if (category == RTVerifierCategory.NONE) {
          // we couldn't categorize this correctly, so let's put the entire visited stack to help us understand why
          visitedFrames.toArray
        } else firstFrame +: interestingFrames :+ lastFrame

        // we used a special category for SI nodes, as they are not an RT violation (yet!)
        val touchedSINode = firstFrame.getMethodName.endsWith("lookupAndGetSI")
        if (touchedSINode) category = RTVerifierCategory.MODULE_CTOR_SI_NODE

        RTVerifierReporter.reportClassViolation(
          category = category,
          key = lastFrame.toString,
          details = frames.mkString("\n"),
          clazzName = lastFrame.getClassName
        )
        interestingFrames
      })
  }

  private def reportIfTransitivelyCached(): Unit = {
    val ec = OGSchedulerContext._TRACESUPPORT_unsafe_current()
    if (ec != null) {
      val ntsk = ec.getCurrentNodeTask
      val isCacheableUserNode =
        ntsk != null && !ntsk.executionInfo().isInternal && ntsk.scenarioStack().cachedTransitively
      if (isCacheableUserNode) {
        val ntskStack = ntsk.nodeStackToCacheable()
        val cacheableNode = ntskStack.lastOption.getOrElse(ntsk)
        // intentionally getting the generated class rather than the runtimeClass class to match the stack trace
        val targetClass = cacheableNode.getClass.getName
        var lastFrame: StackTraceElement = null
        var foundPNodeSync = false

        StackWalker
          .getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
          .walk[Array[AnyRef]]((stream: JStream[StackFrame]) => {
            val interestingFrames = stream
              .dropWhile { frame =>
                val isTrigger =
                  frame.getDeclaringClass == classOf[InstrumentedNotRTFunction] && frame.getMethodName == "trigger"
                !isTrigger
              }
              .skip(1) // dropping the InstrumentedNotRTFunction.trigger frame
              .dropWhile { frame =>
                internalPackages.contains(frame.getDeclaringClass.getPackageName) || frame.getMethodName.endsWith("$_")
              }
              .takeWhile { frame =>
                lastFrame = frame.toStackTraceElement
                val declaringClass = frame.getDeclaringClass
                foundPNodeSync =
                  declaringClass == classOf[PropertyNodeSync[_]] || classOf[AsNode[_]].isAssignableFrom(declaringClass)
                val foundTarget = foundPNodeSync || frame.getClassName.startsWith(targetClass)
                !foundTarget
              }
              .filter(!_.getDeclaringClass.getPackageName.startsWith("scala.runtime.java8"))
              .map[StackTraceElement](_.toStackTraceElement)
              .toArray

            val frames = if (foundPNodeSync) interestingFrames else interestingFrames :+ lastFrame

            val details = {
              val sb = new PrettyStringBuilder()
              sb.append("Node Stack ")
              sb.startBlock()
              ntskStack.forEach { ns =>
                val cleanName =
                  ns.toPrettyName(true, false)
                    // strip lambda encoding so that we can correctly group violations
                    .replaceAll("Lambda./\\w+$", "Lambda./NN")
                sb.appendln(cleanName)
              }
              sb.endBlock()
              sb.append("JVM stack ")
              sb.startBlock()
              frames.foreach(sb.appendln)
              sb.endBlock()
              sb.toString
            }
            val category: String = RTVerifierCategory.NODE_WITH_NON_RT_CALL
            RTVerifierReporter.reportClassViolation(category, cacheableNode.classMethodName(), details, targetClass)
            frames
          })
      }
    }
  }

  // we need to check for both due to the differences between scala 2.12 and 2.13
  private val ctorMethods = List("<init>", "<clinit>")
  private def isFrameOfInterest(frame: StackFrame): String = {
    val clsName = frame.getDeclaringClass.getSimpleName
    val isModule = clsName.endsWith("$")
    val method = frame.getMethodName

    if (isModule && ctorMethods.contains(method)) // It's a module constructor
      RTVerifierCategory.MODULE_CTOR_EC_CURRENT
    else if (isModule && method.endsWith("$lzycompute")) // it's a module with a lazy val
      RTVerifierCategory.MODULE_LAZY_VAL_EC_CURRENT
    else RTVerifierCategory.NONE
  }
}
