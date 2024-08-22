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
package optimus.profiler

import optimus.core.CoreHelpers.safeToString
import optimus.debugger.browser.ui.GraphBrowserAPI._
import optimus.graph.NodeTask
import optimus.graph.NodeTrace
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.loom.LNodeClsID
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.util.html.Annotation
import optimus.platform.util.html.Bold
import optimus.platform.util.html.HtmlBuilder
import optimus.profiler.ui.NodeLineResult

import java.util.prefs.Preferences
import java.util.regex.Pattern
import java.util.regex.Pattern.CASE_INSENSITIVE
import java.util.{ArrayList => JArrayList}
import java.util.{Collection => JCollection}
import java.util.{HashMap => JHashMap}
import java.util.{IdentityHashMap => JIdentityHashMap}
import scala.jdk.CollectionConverters._
import scala.util.Try

object NodeFormatUI {

  /** The default state of the profiler's node formatting capabilities, with no extraneous bells nor whistles. */
  private[optimus] val default = NodeFormatUI(
    useEntityType = false,
    showNodeState = false,
    showKeysInline = false,
    showResult = DebuggerUI.isWindows, // TktToLegIdToLeg.toString can add minutes to debugger refresh on Linux
    showTimings = false,
    abbreviateName = true,
    showCausalityID = false
  )

  // Only visible for testing
  private[optimus] var _current = readFromPref()
  def current: NodeFormatUI = _current
  def current_=(nodeFormatUI: NodeFormatUI): Unit = _current = writeToPref(nodeFormatUI)

  def asStringBuilder: PrettyStringBuilder = {
    val sb = new PrettyStringBuilder()
    sb.useEntityType = current.useEntityType
    sb.showNodeState = current.showNodeState
    sb.showKeys = current.showKeysInline
    sb.simpleName = current.abbreviateName
    sb.showCausalityID = current.showCausalityID
    sb
  }

  private def readFromPref(): NodeFormatUI = {
    val pref = Preferences.userNodeForPackage(NodeFormatUI.getClass)
    val newValue = default.copy()
    val flds = newValue.getClass.getDeclaredFields
    for (fld <- flds) {
      fld.setAccessible(true)
      val v = pref.getBoolean(fld.getName.toLowerCase, fld.getBoolean(default))
      fld.setBoolean(newValue, v)
    }
    // We don't really want to store this field, because it's very unlikely people meant to use it!
    // If they did, they can check it again
    newValue.copy()
  }

  private def writeToPref(nfui: NodeFormatUI): NodeFormatUI = {
    val pref = Preferences.userNodeForPackage(NodeFormatUI.getClass)
    val flds = nfui.getClass.getDeclaredFields
    for (fld <- flds) {
      fld.setAccessible(true)
      pref.putBoolean(fld.getName.toLowerCase, fld.getBoolean(nfui))
    }
    nfui
  }

  // enable the `NodeFormatUI.useEntityType` syntax we're so accustomed to
  implicit def asCurrent(mod: NodeFormatUI.type): NodeFormatUI = current

  /**
   * Walk all nodes reachable from `root`, and assign a unique ID to each argument and result thereof.
   */
  def buildIDHash(root: NodeTask): collection.Map[Any, Int] = {
    try {
      val ids = new JIdentityHashMap[Any, Int]
      var i = 1
      DebuggerUI.foreachDescendant(NodeTrace.accessProfile(root))((n, _) => {
        val onResult = (theResult: Any) => {
          if (!ids.containsKey(theResult)) {
            ids.put(theResult, i)
            i += 1
          }
          val args = n.args
          if (args ne null) {
            args filterNot ids.containsKey foreach { a =>
              ids.put(a, i)
              i += 1
            }
          }
        }

        n.getTask.actOnResult(onResult, _ => (), _ => ())
      })
      ids.asScala
    } catch {
      case ex: Throwable =>
        System.err.println(ex)
        Map.empty
    }
  }

  /** Create a filter with memory for fast matching of nodes */
  def filterNodesFastWithMemory(filter: String): PNodeTask => Boolean = Filter.parse(filter).matcherWithMemory(default)

  def filteredNodes(trace: JCollection[PNodeTask], filterStr: String): JCollection[PNodeTask] =
    filteredNodes(trace, current, filterStr, showInternal = false)

  private[optimus] def filteredNodes(
      trace: JCollection[PNodeTask],
      filterStr: String,
      showInternal: Boolean): JCollection[PNodeTask] =
    filteredNodes(trace, current, filterStr, showInternal)

  private def filteredNodes(
      trace: JCollection[PNodeTask],
      format: NodeFormatUI,
      filterStr: String,
      showInternal: Boolean): JCollection[PNodeTask] = {
    def iterateTasks(predicate: PNodeTask => Boolean): JArrayList[PNodeTask] = {
      val all = new JArrayList[PNodeTask]()
      var i = 0
      val it = trace.iterator()
      while (it.hasNext) {
        val task = it.next()
        if (predicate(task)) all.add(task)
        i += 1
      }
      all
    }

    if ((filterStr eq null) || filterStr.isEmpty) {
      if (showInternal)
        trace
      else
        iterateTasks(!_.isInternal)
    } else {
      val filter = Filter parse filterStr
      val matcher = filter.matcherWithMemory(format)
      iterateTasks(p => matcher(p) && (showInternal || !p.isInternal))
    }
  }
}

/**
 * An ADT to represent the various forms of filtering supported by the debugger UI.
 */
sealed abstract class Filter extends Product with Serializable {

  /** 'memory' avoids re-evaluating matches for the same profileID and subClass */
  final def matcherWithMemory(format: NodeFormatUI): PNodeTask => Boolean = {
    val hash = new JHashMap[(Int, Any), java.lang.Boolean]()

    task: PNodeTask => {
      val subProfileClass = task.subProfile() match {
        // we may not have a subProfile!
        case null                                 => null
        case clsID: LNodeClsID if clsID.isDynamic => clsID._clsID()
        case someObj                              => someObj.getClass
      }
      val key = (task.infoId, subProfileClass)
      var matched = hash.get(key)
      if (matched eq null) {
        matched = run(format)(task)
        hash.put(key, matched)
      }
      matched
    }
  }

  final def run(format: NodeFormatUI)(n: PNodeTask): Boolean = {
    import Filter._
    lazy val formatted = {
      if (n.scenarioStack() ne null)
        DebuggerUI.underStackOf(n.scenarioStack) {
          n.toPrettyName(format.useEntityType, format.abbreviateName)
        }
      else
        n.toPrettyName(format.useEntityType, format.abbreviateName)
    }
    def run(f: Filter): Boolean = f match {
      case Disjunction(subfilters) => subfilters.exists(run)
      case Conjunction(subfilters) => subfilters.forall(run)
      case Negatory(negatee)       => !run(negatee)
      case re @ Regex(_)           => (formatted != null) && re.pattern.matcher(formatted).matches()
      case Id(id)                  => n.infoId() == id
      case Class(name)             => NodeName.cleanNodeClassName(n.nodeClass()) contains name
      case Literal(lit)            => (formatted != null) && formatted.toUpperCase.contains(lit.toUpperCase)
    }
    run(this)
  }
}

object Filter {
  final case class Disjunction(filters: List[Filter]) extends Filter
  final case class Conjunction(filters: List[Filter]) extends Filter
  final case class Negatory(filter: Filter) extends Filter
  final case class Regex(re: String) extends Filter { val pattern: Pattern = Pattern.compile(re, CASE_INSENSITIVE) }
  final case class Id(id: Int) extends Filter
  final case class Class(name: String) extends Filter
  final case class Literal(string: String) extends Filter

  // this is a low-effort parser for the above language; it doesn't support grouping, but we've been fine without it
  def parse(in: String): Filter =
    parse0(in.replaceAll("//.*", "")) match {
      case Left(err)     => throw new IllegalArgumentException(s"invalid filter: $in", err)
      case Right(filter) => filter
    }
  import optimus.utils.MiscUtils.Traversablish
  private def parse0(in: String): Either[Throwable, Filter] = in match {
    case disj if disj contains '|' =>
      disj.split('|').toList.traverseEither(parse0).map(Disjunction)
    case conj if conj contains '&' =>
      conj.split('&').toList.traverseEither(parse0).map(Conjunction)
    case nega if nega startsWith "!" =>
      parse0(nega drop 1).map(Negatory)
    case re if re startsWith "r'" =>
      Try(Regex(re.drop(2).trim)).toEither
    case id if id startsWith "%" =>
      Try(Id(id.tail.trim.toInt)).toEither
    case cls if cls startsWith "class:" =>
      Right(Class(cls drop 6))
    case lit =>
      Right(Literal(lit))
  }
}

/**
 * A configurable formatter and filterer for [[NodeTask]] objects.
 *
 * A static mutable instance lives in [[NodeFormatUI]], where it is used by the Swing parts of the profiler. Having an
 * immutable class (and therefore not forcing reliance on global state) eases testing.
 */
final case class NodeFormatUI(
    useEntityType: Boolean,
    showNodeState: Boolean,
    showKeysInline: Boolean,
    showResult: Boolean,
    showTimings: Boolean,
    abbreviateName: Boolean,
    showCausalityID: Boolean
) {

  def formatName(ntsk: NodeTask): String =
    DebuggerUI.underStackOf(ntsk.scenarioStack) {
      safeToString(ntsk.toPrettyName(useEntityType, showKeysInline, showNodeState, abbreviateName, showCausalityID))
    }

  def formatLine(node: NodeTask, hb: HtmlBuilder, ids: collection.Map[Any, Int] = null): HtmlBuilder = {
    DebuggerUI.underStackOf(node.scenarioStack) {
      if (DebuggerUI.custom_formatLine ne null) {
        hb.freeform(DebuggerUI.custom_formatLine(node))
      } else {
        def formatValue(a: Any) = {
          if ((ids ne null) && ids.contains(a)) {
            val id = ids(a)
            if (id > 0) {
              hb ++= Annotation(s"[$id]").noSeparator()
            }
          }
          hb ++= ("AnnotatedValue", safeToString(a, "..."))
        }

        hb ++= ("NodeName", formatName(node))

        if (showTimings)
          hb ++= ("NodeTimings", s"${node.scaledWallTime}/${node.scaledSelfTime} ")

        hb.styledGroup(nodes => NodeLineResult(nodes: _*)) {
          node.actOnResult(formatValue, th => hb.noStyle(th.toString), hb.noStyle(_))
        }

        val args = node.args
        if (args.nonEmpty) {
          hb.namedGroup("NodeArguments") {
            args.foreach { arg =>
              hb ++= Bold("(").noSeparator()
              formatValue(arg)
              hb.removeSeparator()
              hb ++= Bold(")")
            }
          }
        }
      }
      hb
    }
  }
}
