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
/* package optimus.tools.scalacplugins.entity

import java.util.regex.Pattern
import scala.reflect.internal.util.Position
import scala.reflect.io.AbstractFile
import scala.tools.nsc.Global
import scala.tools.nsc.Phase
import scala.tools.nsc.profile.ProfileSnap
import scala.tools.nsc.profile.Profiler

/** a lightweight profiler which reports when typing a particular symbol takes over a specified threshold of time */
class ThresholdProfiler(reporter: (Position, String) => Unit, thresholdNs: Long) extends Profiler {
  override def finished(): Unit = ()
  override def beforePhase(phase: Phase): ProfileSnap = ProfileSnap(0, "", 0, 0, 0, 0, 0, 0, 0, 0)
  override def afterPhase(phase: Phase, profileBefore: ProfileSnap): Unit = ()
  override def beforeUnit(phase: Phase, file: AbstractFile): Unit = ()
  override def afterUnit(phase: Phase, file: AbstractFile): Unit = ()

  // stack of nanoTimes of beforeTypedImplDef calls
  private var timeStack: List[Long] = Nil

  override def beforeTypedImplDef(sym: Global#Symbol): Unit = {
    timeStack ::= System.nanoTime()
  }

  override def afterTypedImplDef(sym: Global#Symbol): Unit = {
    val startTime = timeStack.head
    timeStack = timeStack.tail
    val duration = System.nanoTime() - startTime
    if (duration > thresholdNs) {
      val fullName = sym.ownerChain.map(_.nameString).reverse.mkString(".")
      val location = Option(sym.sourceFile).map(_.path).getOrElse("<no source file>")
      reporter(sym.pos, ThresholdProfiler.MessageString(s"$fullName in $location", duration / 1000000000d))

      // Since we've already attributed this time, it's confusing to also blame the path that got us to here (which
      // is not necessarily just the enclosing owners, because the typechecker jumps to callees when needed). So we
      // deduct this time from the path by pushing forward the start times by a corresponding amount
      timeStack = timeStack.map(_ + duration)
    }
  }
}

/** delegates to one or more profilers */
class DelegatingProfiler(delegates: Seq[Profiler]) extends Profiler {
  require(delegates.nonEmpty)

  override def finished(): Unit = delegates.foreach(_.finished())

  override def beforePhase(phase: Phase): ProfileSnap = {
    // there's no meaningful way to combine snaps, so just return the first
    val snap = delegates.head.beforePhase(phase)
    delegates.tail.foreach(_.beforePhase(phase))
    snap
  }
  override def afterPhase(phase: Phase, profileBefore: ProfileSnap): Unit =
    delegates.foreach(_.afterPhase(phase, profileBefore))

  override def beforeUnit(phase: Phase, file: AbstractFile): Unit = delegates.foreach(_.beforeUnit(phase, file))
  override def afterUnit(phase: Phase, file: AbstractFile): Unit = delegates.foreach(_.afterUnit(phase, file))
  override def beforeTypedImplDef(sym: Global#Symbol): Unit = delegates.foreach(_.beforeTypedImplDef(sym))
  override def afterTypedImplDef(sym: Global#Symbol): Unit = delegates.foreach(_.afterTypedImplDef(sym))

  override def beforeImplicitSearch(pt: Global#Type): Unit = delegates.foreach(_.beforeImplicitSearch(pt))
  override def afterImplicitSearch(pt: Global#Type): Unit = delegates.foreach(_.afterImplicitSearch(pt))

  override def beforeMacroExpansion(macroSym: Global#Symbol): Unit = delegates.foreach(_.beforeMacroExpansion(macroSym))
  override def afterMacroExpansion(macroSym: Global#Symbol): Unit = delegates.foreach(_.afterMacroExpansion(macroSym))

  override def beforeCompletion(root: Global#Symbol, associatedFile: AbstractFile): Unit =
    delegates.foreach(_.beforeCompletion(root, associatedFile))
  override def afterCompletion(root: Global#Symbol, associatedFile: AbstractFile): Unit =
    delegates.foreach(_.afterCompletion(root, associatedFile))
}

object ThresholdProfiler {

  /**
   * Constructs and deconstructs the special human readable messages about slow compilation. NOTE: this is used by the
   * compiler plugins and by OBT. If you change the format, the special treatment of these messages in OBT will stop
   * working until the next OBT release.
   */
  object MessageString {
    val MsgStart = "[SLOW COMPILATION] "
    private val MsgMiddle = " took "
    private val MsgEnd = " seconds to typecheck"
    private val Regex = (s"${Pattern.quote(MsgStart)}(.*)$MsgMiddle([0-9]+\\.?[0-9]*)$MsgEnd").r

    def apply(symbolName: String, durationInSeconds: Double): String =
      apply(symbolName, f"$durationInSeconds%.2f")

    def apply(symbolName: String, durationInSecondsStr: String): String =
      s"$MsgStart$symbolName$MsgMiddle$durationInSecondsStr$MsgEnd"

    def unapply(msg: String): Option[(String, Double)] = {
      // quick check first
      if (msg.startsWith(MsgStart)) {
        msg match {
          case Regex(symbolName, durationInSeconds) => Some((symbolName, durationInSeconds.toDouble))
          case _                                    => None
        }
      } else None
    }
  }
} */
