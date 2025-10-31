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
package optimus.graph.diagnostics

import optimus.debug.InstrumentationConfig.MethodRef
import optimus.graph.DiagnosticSettings
import optimus.graph.NodeResultNode
import optimus.graph.NodeTask
import optimus.graph.NodeTaskInfo
import optimus.graph.OGTrace
import optimus.graph.ProxyPropertyNode
import optimus.graph.SourceLocator
import optimus.graph.TweakNode
import optimus.graph.diagnostics.ap.StackAnalysis.CleanName
import optimus.graph.loom.LNodeClsID
import optimus.graph.loom.LoomConfig.COLUMN_NA
import optimus.graph.loom.LPropertyDescriptor

import java.util.concurrent.ConcurrentHashMap
import java.util.function

//dont use scala StringBuilder
import java.lang.{StringBuilder => JStringBuilder}

final case class NodeName(pkgName: String, name: String, modifier: String = "") {
  def toString(shortened: Boolean, includeHint: Boolean): String = {
    val asString =
      if (shortened) NodeName.fullNamePackageShortened(pkgName, name, modifier)
      else NodeName.fullName(pkgName, name, modifier)
    if (includeHint) {
      val hint = NodeName.helpfulTranslations.get(this)
      if (hint.isDefined) asString + " (" + hint.get + ")"
      else asString
    } else asString
  }
  def nameAndModifier: String = name + modifier
  def toString(shortened: Boolean): String = toString(shortened, includeHint = false)
  override def toString: String = toString(shortened = false)

  def contains(text: String): Boolean = toString.contains(text)

  /** for comparing e.g. proxies */
  def underlying: NodeName = copy(modifier = "")
}

object NodeName {
  val dalCall: NodeName = NodeName("optimus.platform.dal", "DALDSIExecutor.doExecuteQuery")
  val getEntityByKeyAtNowOption: NodeName = NodeName("optimus.graph", "PluginSupport.getEntityByKeyAtNowOption")

  private val helpfulTranslations: Map[NodeName, String] = Map(getEntityByKeyAtNowOption -> "getOption")

  def apply(ref: MethodRef): NodeName = {
    val indexOfClassName = ref.cls.lastIndexOf('/')
    val clsName = ref.cls.substring(indexOfClassName + 1)
    val pkgName = ref.cls.substring(0, indexOfClassName).replace('/', '.')
    NodeName(pkgName, clsName + "." + ref.method)
  }

  private def stripPackageFromName(pkgName: String, name: String): String = {
    if (name.startsWith(pkgName) && name.charAt(pkgName.length) == '.') {
      name.substring(pkgName.length + 1)
    } else name
  }

  def cleanNodeClassName(a: AnyRef): String = if (a ne null) cleanNodeClassName(a.getClass) else "null"

  private[this] val cleanNodeClassNameCache: ConcurrentHashMap[Class[_], String] = new ConcurrentHashMap()
  private[this] val cleanNodeClassNameComputer: function.Function[Class[_], String] =
    cls => cleanNodeClassName(cls.getName, '.')

  def cleanNodeClassName(cls: Class[_]): String =
    if (cls eq null) "<unknown>" else cleanNodeClassNameCache.computeIfAbsent(cls, cleanNodeClassNameComputer)

  def cleanNodeClassName(clsName: String, replaceWith: Char): String =
    if (clsName eq null) "null"
    else {
      val sb = new JStringBuilder
      var i = 0

      def eatString(i: Int, s: String): Int = {
        val newStart = i + s.length
        // only eat the string if it is followed by a non-letter/digit character or end of string (this is so that
        // we don't eat $node from the classname of "@node def nodesAreFun" etc.)
        if (clsName.startsWith(s, i) && (newStart >= clsName.length || !clsName(newStart).isLetterOrDigit))
          newStart
        else i
      }

      def c(i: Int): Char = if (i < clsName.length()) clsName(i) else 0

      while (i < clsName.length()) {
        // Hidden class suffix and propertyID suffix mark the end of any class
        if (c(i) == '/' || c(i) == '-') i = clsName.length
        else if (c(i) != '$') {
          sb.append(clsName(i))
          i += 1
        } else {
          if (sb.length > 0 && sb.charAt(sb.length - 1) != replaceWith && c(i + 1) != replaceWith)
            sb.append(replaceWith)

          if (c(i + 1).isDigit) {
            i += 1
            if (DiagnosticSettings.profileOverloads) {
              if (c(i) == '1') i += 1 // Only $1, the $2 etc. are overloads
            } else while (c(i).isDigit) i += 1 // Eat '$' and followup digits
          } else {
            var k = i
            k = eatString(k, "$anonfun")
            k = eatString(k, "$node")
            k = eatString(k, "$stateMachine")
            k = eatString(k, "$macro")
            k = eatString(k, "$async")
            i = if (k == i) i + 1 else k
          }
        }
      }
      if (sb.length > 1 && sb.charAt(sb.length - 1) == replaceWith)
        sb.setLength(sb.length - 1)

      sb.toString
    }

  def asStackTraceElement(cls: Class[_]): StackTraceElement = {
    val te = SourceLocator.traceElement(cls)
    new StackTraceElement(cleanNodeClassName(cls), "", te.getFileName, te.getLineNumber)
  }

  private[this] val nameAndSourceCache: ConcurrentHashMap[Class[_], String] = new ConcurrentHashMap()
  private[this] val nameAndSourceComputer: function.Function[Class[_], String] =
    cls => cleanNodeClassName(cls) + " (" + SourceLocator.sourceOf(cls) + ")"

  def nameAndSource(f: Any): String =
    f match {
      case clsID: LNodeClsID if clsID.isDynamic =>
        val ste = clsID.stackTraceElem()
        val clsName = cleanNodeClassName(ste.getClassName, '.')
        clsName + "." + ste.getMethodName + " (" + ste.getFileName + ":" + ste.getLineNumber + ")"
      case _ => nameAndSource(f.getClass)
    }
  def nameAndSource(cls: Class[_]): String = nameAndSourceCache.computeIfAbsent(cls, nameAndSourceComputer)

  def printSource(profileID: Int): Unit = {
    val pnti = OGTrace.trace.getCollectedPNTI(profileID)
    if (pnti ne null)
      printSource(pnti)
  }

  def printSource(pnt: PNodeTask): Unit =
    if (pnt.isLive) println(nameAndSource(pnt.getTask.getClass)) else println(s"${pnt.nodeName} [recorded]")

  def printSource(pnti: PNodeTaskInfo): Unit = {
    val hint = helpfulTranslations.getOrElse(pnti.nodeName, "")
    val hintStr = if (hint.nonEmpty) s" [$hint]" else ""
    if (pnti.nodeCls ne null) println(nameAndSource(pnti.nodeCls) + hintStr)
  }

  def printSource(obj: AnyRef): Unit = {
    // This is not a log! It prints to console because console is normally intercepted by IDE and this is where we want the output
    println(detailedSource(obj))
  }

  private[optimus] def detailedSource(obj: AnyRef): String = {
    nameAndSource(obj.getClass) + {
      obj match {
        case twkn: TweakNode[_] => "\ntweak defined at: " + twkn.tweakLocation
        case ppn: ProxyPropertyNode[_] =>
          "\nnode source at: " + nameAndSource(ppn.srcNodeTemplate.getClass)
        case nrn: NodeResultNode[_] =>
          "\nnode source at: " + nameAndSource(nrn.childNode.getClass)
        case _ => ""
      }
    }
  }

  def fullNamePackageShortened(pkgName: String, name: String, modifier: String): String =
    shortPackageName(pkgName) + name + modifier

  def fullName(pkgName: String, name: String, modifier: String): String = {
    if (pkgName == null || pkgName.isEmpty) name + modifier else pkgName + "." + name + modifier
  }

  private def shortPackageName(pkgName: String): String = CleanName.shortPackageName(pkgName)

  def fromNodeCls(nodeCls: Class[_]): NodeName = fromNodeCls(nodeCls, "")
  def fromNodeCls(nodeCls: Class[_], modifier: String): NodeName = {
    if (nodeCls != null) {
      val pkgName = nodeCls.getPackage.getName
      val name = NodeName.stripPackageFromName(pkgName, NodeName.cleanNodeClassName(nodeCls))
      NodeName(pkgName, name, modifier)
    } else {
      NodeName("", "[error]", modifier)
    }
  }

  def fromSubProfile(f: Any): NodeName = f match {
    case clsID: LNodeClsID if clsID.isDynamic => from(clsID.stackTraceElem())
    case _                                    => fromNodeCls(f.getClass)
  }

  /** Returns NodeName suitable for display in graph debugger/profiler */
  def from(ntsk: NodeTask): NodeName = ntsk match {
    case clsID: LNodeClsID if clsID.isDynamic => fromClsID(clsID)
    case _                                    => from(ntsk.executionInfo(), ntsk.getClass)
  }

  private def fromClsID(clsID: LNodeClsID): NodeName = {
    val desc = LPropertyDescriptor.get(clsID._clsID())
    val nn = from(clsID.stackTraceElem())
    // This logic matches that of from(ntsk.executionInfo(), class).
    if (desc.columnNumber < 0 && desc.columnNumber != COLUMN_NA)
      nn.copy(modifier = NodeTaskInfo.StoredNodeFunction.rawName())
    else nn
  }

  def from(stackElm: StackTraceElement): NodeName = {
    val clsName = stackElm.getClassName
    val dotIndex = clsName.lastIndexOf(".")
    if (dotIndex == -1) new NodeName(clsName, stackElm.getMethodName)
    else new NodeName(clsName.substring(0, dotIndex), clsName.substring(dotIndex + 1) + "." + stackElm.getMethodName)
  }

  /**
   * Returns a NodeName which will be correct for all nodes with the same .getProfileId value.
   *
   * Nodes that do not have a unique profile id (e.g. CompletableNode extensions) will return [default].
   */
  def profileFrom(ntsk: NodeTask): NodeName =
    if (ntsk.getProfileId == ntsk.executionInfo().profile) ntsk.executionInfo().nodeName()
    else from(ntsk.executionInfo(), ntsk.getClass)

  /**
   * Rationale: [SEE_FREEZING_NODE_NAMES]
   *   1. Internal graph names get their short names (we don't really want user to pay attention where they are)
   *   1. Most node tasks get their runtime class (including proxy nodes, which DO have runtime class even though they
   *      are marked 'internal')
   *   1. Otherwise if nti runtime class name is null we use nodeCls - this includes all user-generated tasks like given
   *      blocks and RHS lambdas etc
   */
  def from(nti: NodeTaskInfo, nodeCls: Class[_]): NodeName = {
    if (nti != null) {
      if (nti.runtimeClass() != null) {
        val pkgName = nti.runtimeClass().getPackage.getName
        val name = NodeName.stripPackageFromName(pkgName, nti.runtimeClass().getName + "." + nti.rawName())
        NodeName(pkgName, name, nti.modifier())
      } else if (nti.isInternal || nodeCls == null)
        NodeName("", nti.rawName(), nti.modifier())
      else if (nti == NodeTaskInfo.StoredNodeFunction) // useful to see [node-function] in the debugger
        fromNodeCls(nodeCls, nti.rawName())
      else
        fromNodeCls(nodeCls)
    } else
      fromNodeCls(nodeCls)
  }
}
