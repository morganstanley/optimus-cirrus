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
package optimus.profiler.ui

import java.awt.Color
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.util.prefs.Preferences
import javax.swing.table.TableCellRenderer
import optimus.EntityAgent
import optimus.core.CoreHelpers
import optimus.debug.InstrumentationConfig
import optimus.graph.AlreadyCompletedPropertyNode
import optimus.graph.SourceLocator
import optimus.graph.InstancePropertyTarget
import optimus.graph.MutableSSCacheID
import optimus.graph.NodeTask
import optimus.graph.PredicatedPropertyTweakTarget
import optimus.graph.RecordedTweakables
import optimus.graph.TweakNode
import optimus.graph.TweakTreeNode
import optimus.graph.diagnostics.DbgPreference
import optimus.graph.diagnostics.SelectionFlags
import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.SelectionFlags.SelectionFlag
import optimus.platform.NodeHash
import optimus.platform.Scenario
import optimus.platform.ScenarioStack
import optimus.platform.Tweak
import optimus.platform.storable.Entity
import optimus.profiler.DebuggerUI
import optimus.profiler.MarkObject
import optimus.profiler.ui.NPTableRenderer.ValueWithTooltip
import optimus.profiler.ui.ValueTreeTable.RowState
import optimus.profiler.ui.common.JPopupMenu2

import java.util.Objects
import scala.collection.compat._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable => m}
import scala.util.control.NonFatal

class RowID(val id: Any, val defOpen: Boolean = false) extends Ordered[RowID] {
  def typePrecedence: Int = 0
  def compareID(that: RowID): Int = id.toString.compareTo(that.id.toString)
  def title(raw: Boolean): String = id.toString
  def foreground: Color = null
  override def hashCode(): Int = id.##
  override def equals(obj: Any): Boolean = id == obj.asInstanceOf[RowID].id
  override def compare(that: RowID): Int = {
    val diffType = that.typePrecedence - typePrecedence
    if (diffType == 0) compareID(that) else diffType
  }
}
class RowIDInt(i: Int) extends RowID(i) {
  override def typePrecedence: Int = 1
  override def compareID(that: RowID): Int = i - that.id.asInstanceOf[Int]
}
class RowIDString(i: String, defOpen: Boolean) extends RowID(i, defOpen) { override def typePrecedence: Int = 2 }
class RowIDField(f: Field) extends RowID(f) {
  override def typePrecedence: Int = 3
  override def title(raw: Boolean): String = {
    val fieldName = f.getName
    if (raw) fieldName
    else {
      val index = fieldName.lastIndexOf('$')
      if (index > 0) {
        // ie, if it contains '$$', just drop everything up to that, the 'real name' follows
        if ('$'.equals(fieldName.charAt(index - 1)))
          fieldName.drop(index + 1)
        else
          // otherwise, if field name ends with $n for some int n, drop it, the 'real name' is the prefix
          fieldName.substring(0, index)
      } else fieldName
    }
  }
}
class RowIDNode(p: AnyRef, defOpen: Boolean) extends RowID(p, defOpen) {
  override def typePrecedence: Int = 4
}
class RowIDScenarioStack(stack: ScenarioStack) extends RowIDString("Scenario", true) {
  override def typePrecedence: Int = -1
  override def foreground: Color = if (stack._cacheID.isInstanceOf[MutableSSCacheID]) Color.red else null
}
object RowIDScenario extends RowIDString("Scenario", true) {
  override def typePrecedence: Int = -1
}
object RowID {
  def apply(v: Int): RowID = new RowIDInt(v)
  def apply(v: String): RowID = new RowIDString(v, false)
  def apply(v: Field): RowID = new RowIDField(v)
  def apply(v: AnyRef, defOpen: Boolean): RowID = new RowIDNode(v, defOpen)
  def apply(v: ScenarioStack): RowID = new RowIDScenarioStack(v)
}

class ValueViewRow(id: RowID, val values: Array[Any], val diffs: SelectionFlag, var level: Int) extends NPTreeNode {
  var children: Array[ValueViewRow] = _
  def rowID: RowID = id
  open = id.defOpen
  override def tooltipText: String = SelectionFlags.toToolTipText(diffs)

  override def title: String = rowID.title(ValueTreeTable.showRawNames.get)

  private def extractValues(v: Any, fields: m.HashMap[RowID, Any]): Unit = {
    v match {
      case e: Entity =>
        // two entities are only the same if they have the same storage info
        fields.put(RowID("storageInfo"), e.dal$storageInfo)
        // For entities, we always want to see their fields, even if they eg extend Iterable
        ValueTreeTable.fieldExpander(v, fields)
      case arr: Array[_] =>
        for (idx <- arr.indices) {
          val rowIndex = RowID(idx)
          fields.put(rowIndex, arr(idx))
        }
      case seq: Iterable[_] =>
        for ((elem, idx) <- seq.zipWithIndex) {
          val name = RowID(idx)
          fields.put(name, elem)
        }
      case v if !ValueTreeTable.showRawValues.get && v != null && ValueTreeTable.customExpander.contains(v.getClass) =>
        ValueTreeTable.customExpander(v)(v, fields)
      case _: ScenarioStack =>
        // ScenarioStack should always use custom expander even if (showRawValues == true)
        ValueTreeTable.customExpander(v)(v, fields)
      case node: NodeTask =>
        ValueTreeTable.fieldExpander(node, fields)
        if (!ValueTreeTable.showInternalValues.get) {
          if (node.isDoneWithException)
            fields.put(RowID("Exception"), node.exception())
          else
            fields.put(RowID("Result"), node.resultObjectEvenIfIncomplete())
        }
      case v if (v != null) && !ValueTreeTable.ignoreClasses.contains(v.getClass) =>
        ValueTreeTable.fieldExpander(v, fields)
      case _ => /* null or one of the ignored classes */
    }
  }

  def refreshChildren(currentRowStates: Map[RowID, RowState] = Map.empty[RowID, RowState]): Unit = {
    val idSet = m.HashSet.empty[RowID]
    val fieldMapForColumn = new Array[m.HashMap[RowID, Any]](values.length)
    for (vi <- values.indices) { // For each column (ie each node we are comparing)
      val fields = m.HashMap.empty[RowID, Any] // Buckets of values (ie fields to display for each node)
      fieldMapForColumn(vi) = fields
      extractValues(values(vi), fields) // this is the field expansion that depends on selected config
      idSet ++= fields.keys
    }

    val unsortedChildren = idSet.iterator.map { rowID =>
      val newValues = new Array[Any](values.length)
      val currentRowState = currentRowStates.get(rowID)
      var valsDiffer = SelectionFlags.NotFlagged
      for (vi <- values.indices) {
        val valueOfCurrentRow = fieldMapForColumn(vi).get(rowID)
        if (valueOfCurrentRow.isDefined) {
          val fld = valueOfCurrentRow.get
          newValues(vi) = fld
          newValues(vi) match {
            case value: AnyRef =>
              for (v <- values if v.isInstanceOf[AnyRef])
                if (v.asInstanceOf[AnyRef] eq value) newValues(vi) = null // avoid endless recursion
            case _ =>
          }

          if (currentRowState.isDefined) // don't redo the comparison if we already have it for a given row
            valsDiffer = currentRowState.get.hasDiffs
          else if (vi > 0 && valsDiffer == SelectionFlags.NotFlagged) {
            val currentValue = newValues(vi)
            val compareTo = newValues(0)
            valsDiffer = ValueTreeTable.rowsAreDifferent(currentValue, compareTo)
          }
        } else if (vi > 0 && newValues(0) != null) {
          valsDiffer = SelectionFlags.EqualDiff
        }
      }
      val newRow = new ValueViewRow(rowID, newValues, valsDiffer, level + 1)
      // if we had a corresponding previous child, keep its expanded/collapsed state, otherwise take the rowID default
      if (currentRowState.isDefined)
        newRow.open = currentRowState.get.open
      newRow
    }.toIndexedSeq

    children = unsortedChildren.sortBy(_.rowID).toArray
  }

  def getChildren: Iterable[NPTreeNode] =
    if (children ne null) children
    else if (values.isEmpty) List.empty
    else {
      refreshChildren()
      children
    }

  def hasChildren: Boolean = getChildren.nonEmpty

  override def background: Color = diffFlag match {
    case SelectionFlags.Outlier    => Color.RED
    case SelectionFlags.Irrelevant => Color.PINK
    case SelectionFlags.Divergent  => Color.ORANGE
    case _                         => super.background
  }

  override def foreground: Color = diffFlag match {
    case SelectionFlags.Irrelevant => Color.DARK_GRAY
    case _                         => rowID.foreground
  }

  override def toString: String = title // so that copying ValueViewRows produces useful output
}

object ValueTreeTable {
  private val pref = Preferences.userNodeForPackage(ValueTreeTable.getClass)
  private val MAX_TO_COMPARE = 5
  val VALUE_TABLE_WIDTH = "valueTableWidth"
  val NAME_COLUMN_WIDTH = "nameColumnWidth"

  private[ui] val showRawValues = DbgPreference("showRawValues", pref)
  private[ui] var showInternalValues = DbgPreference("showInternalValues", pref)
  private[ui] var showRawNames = DbgPreference("showRawNames", pref)

  def calculateAverageColumnWidth(totalColumnWidth: Int, nameColumnWidth: Int, totalColumnCount: Int): Int = {
    if (nameColumnWidth != 0 && totalColumnCount != 1) {
      (totalColumnWidth - nameColumnWidth) / (totalColumnCount - 1)
    } else {
      totalColumnWidth / totalColumnCount
    }
  }

  def hidePackage(cls: Class[_]): Boolean = {
    if (ValueTreeTable.showInternalValues.get) false
    else {
      val packageName = cls.getPackage.getName
      (packageName.startsWith("optimus.graph") || packageName.startsWith("optimus.platform")) &&
      !cls.getSimpleName.startsWith("NodeFunction")
    }
  }

  val diffColorBgSelected = new Color(215, 215, 255)
  val ignoreClasses: Set[Class[_]] = Set(
    classOf[java.lang.Boolean],
    classOf[Boolean],
    classOf[java.lang.Integer],
    classOf[java.lang.Long],
    classOf[java.lang.Float],
    classOf[java.lang.Double],
    classOf[java.lang.Byte],
  )

  type ToString[-T] = T => String
  private[ui] val customValueToString: ClassDispatcher[ToString] =
    ClassDispatcher
      .newBuilder[ToString]
      .put[TweakTreeNode] { ttn =>
        if (ttn.tweak != null) ttn.tweak.tweakTemplate.toString
        else ttn.toString
      }
      .result()

  type CustomExpanderF[-T] = (T, m.HashMap[RowID, Any]) => Unit
  private[ui] val customExpander: ClassDispatcher[CustomExpanderF] = ClassDispatcher
    .newBuilder[CustomExpanderF]
    .put[NodeHash] { (nh, fields) =>
      if (nh.dbgKey != null) {
        // Expand as node if possible
        ValueTreeTable.fieldExpander(nh.dbgKey, fields)
      } else {
        fields.put(RowID("property"), nh.property.fullName())
        fields.put(RowID("entity hash"), s"#${nh.entityHash.toHexString}")
        fields.put(RowID("args hash"), s"#${nh.argsHash.toHexString}")
      }
    }
    .put[RecordedTweakables]((rt, fields) => {
      // Compare as a set because that is how we decide to cache or not, so it's more useful in our Tweak Dependencies
      // view:
      if (rt.tweakable.nonEmpty) fields.put(RowID("tweakable"), rt.tweakable.to(Set))
      if (rt.tweakableHashes.nonEmpty) fields.put(RowID("tweakableHashes"), rt.tweakableHashes.to(Set))

      for (ttn <- rt.tweaked) {
        val tweak = ttn.tweak
        val key = if (tweak ne null) tweak.target else ttn.key // consider underStackOf for target?
        val value = ttn
        val defOpen = if (ttn.nested eq null) false else !ttn.nested.isEmpty
        fields.put(new RowIDNode(key, defOpen), value)
      }
    })
    .put[TweakTreeNode]((ttn, fields) => {
      fields.put(RowID("tweak"), ttn.tweak)
      if (ttn.nested != null && !ttn.nested.isEmpty) fields.put(RowIDScenario, ttn.nested)
    })
    .put[Tweak]((tweak, fields) => {
      fields.put(RowID("target"), tweak.target)
      fields.put(RowID("tweakTemplate"), tweak.tweakTemplate)
    })
    .put[TweakNode[_]]((tn, fields) => {
      fields.put(RowID("computeGenerator"), new ComputeGenViewer(tn.computeGenerator))
    })
    .put[InstancePropertyTarget[_, _]]((ipt, fields) => {
      fields.put(RowID("key"), ipt.key)
    })
    .put[PredicatedPropertyTweakTarget[_, _, _, _]]((ppt, fields) => {
      fields.put(RowID("propertyInfo"), ppt.propertyInfo)
      fields.put(RowID("predicate"), ppt.predicate)
    })
    .put[Scenario]((scen, fields) => scenarioExpander(null, scen, fields, includeNested = true))
    .put[ScenarioStack]((ss, fields) => scenarioStackExpander(ss, fields))
    .result()

  def fieldExpander(v: Any, fields: m.HashMap[RowID, Any]): Unit = {
    var cls = v.getClass
    while (cls != classOf[Object] && !ValueTreeTable.hidePackage(cls)) {
      val flds = cls.getDeclaredFields
      for (fi <- 0 until flds.length) {
        val fld = flds(fi)
        if (!Modifier.isStatic(fld.getModifiers) && fld.getName != "this$0") {
          try {
            fld.setAccessible(true)
            val fieldValue = fld.get(v)
            fieldValue match {
              case acpn: AlreadyCompletedPropertyNode[_] => fields.put(RowID(fld), acpn.resultObjectEvenIfIncomplete)
              case _                                     => fields.put(RowID(fld), fieldValue)
            }
          } catch {
            case NonFatal(e) => fields.put(RowID(fld), e.getMessage)
          }
        }
      }
      cls = cls.getSuperclass
    }
  }

  def scenarioStackExpander(ss: ScenarioStack, fields: m.HashMap[RowID, Any]): Unit = {
    if ((ss.topScenario ne null) && !ss.topScenario.isEmptyShallow)
      scenarioExpander(ss, ss.topScenario, fields, includeNested = false)
    else
      tweakExpander(ss, ss.expandedTweaks, fields)

    if (ss.parent ne null)
      fields.put(RowID(ss.parent), ss.parent)
  }

  def scenarioExpander(
      ss: ScenarioStack,
      scenario: Scenario,
      fields: m.HashMap[RowID, Any],
      includeNested: Boolean): Unit = {
    tweakExpander(ss, scenario.topLevelTweaks, fields)
    if (includeNested && scenario.nestedScenarios.nonEmpty)
      fields.put(RowIDScenario, scenario.withNestedAsScenario)
  }

  def tweakExpander(ss: ScenarioStack, tweaks: Iterable[Tweak], fields: m.HashMap[RowID, Any]): Unit =
    DebuggerUI.underStackOf(ss) {
      for (twk <- tweaks)
        fields.put(RowID(twk.target.toString), twk.tweakTemplate) // for toString calling onto graph
    }

  // helper to compare arrays properly in diff view
  def rowsAreDifferent(currentValue: Any, compareTo: Any): SelectionFlag = {
    def selectFlags(a: Any, b: Any): SelectionFlag = {
      // null+null case already checked
      if (Objects.isNull(a) ^ Objects.isNull(b)) SelectionFlags.EqualDiff
      else if (a != b) SelectionFlags.EqualDiff // not equal
      else if (a.hashCode != b.hashCode) SelectionFlags.HashCodeDiff // equal, but hashcode is different
      else SelectionFlags.NotFlagged
    } // equal, same hashcode

    def arrayDiff(a: Array[_], b: Array[_]) = {
      def arrayType(obj: Array[_]): Class[_] = obj.getClass.getComponentType

      if (arrayType(a) != arrayType(b)) SelectionFlags.EqualDiff // different array types
      else if (a.length != b.length) SelectionFlags.EqualDiff // different lengths
      else if (a ne b) SelectionFlags.EqualDiff
      else SelectionFlags.NotFlagged
    }

    (currentValue, compareTo) match {
      case (null, null)               => SelectionFlags.NotFlagged
      case (a: Array[_], b: Array[_]) => arrayDiff(a, b)
      case _                          => selectFlags(currentValue, compareTo) // nothing special if they're not arrays
    }
  }

  private def makeNameColumn(nameColumnWidth: Int): TableColumn[ValueViewRow] =
    new TableColumn[ValueViewRow]("Name", nameColumnWidth) {
      override def valueOf(row: ValueViewRow): ValueViewRow = row
      override def getCellRenderer: TableCellRenderer = NPTableRenderer.treeRenderer
    }

  class ValueTableColumn(title: String, val index: Int, val columnWidth: Int)
      extends TableColumn[ValueViewRow](title, columnWidth) {
    override def getCellCustomColor(selected: Boolean, row: ValueViewRow, row2: ValueViewRow): Color = {
      row.diffs match {
        case SelectionFlags.HashCodeDiff =>
          if (selected) diffColorBgSelected else NPTableRenderer.lightOrange
        case SelectionFlags.EqualDiff =>
          if (selected) diffColorBgSelected else NPTableRenderer.lightYellow
        case _ => null
      }
    }

    override def valueOf(row: ValueViewRow): ValueWithTooltip[String] = {
      val value = row.values(index)
      val text =
        if (value == null) ""
        else if (customValueToString.contains(value.getClass)) customValueToString(value)(value)
        else CoreHelpers.safeToString(value)

      value match {
        case nodeTask: NodeTask if MarkObject.inMarkedObjects(nodeTask) =>
          val labelledText = MarkObject.formatMarkedObject(nodeTask, text)
          ValueWithTooltip(labelledText, row.tooltipText)
        case _ => ValueWithTooltip(text, row.tooltipText)
      }
    }
  }

  final case class RowState(open: Boolean, hasDiffs: SelectionFlag)

  final class ComputeGenViewer(computeGen: AnyRef) {
    override def equals(other: Any): Boolean = TweakNode.areGeneratorsCompatible(computeGen, other)
    override def hashCode: Int = computeGen.hashCode()
  }
}

class ValueTreeTable(title: String = "value") extends NPTreeTable[ValueViewRow] {
  import optimus.profiler.ui.ValueTreeTable._

  // There is an annoying bit of state here, which is that we have to maintain column widths manually.
  //
  // There are two main "flows" where we want the behaviour to be user friendly:
  //   - user is looking at values for one node, they select a second node to compare (or a third, or fourth etc.)
  //   - user is clicking through nodes because they want to look at different values.
  //
  // This means that we want to make sure that the "values" columns have widths that don't change when we change the
  // selection.
  //
  // The name column has a single width which is saved and restored from a preference.
  private def defaultNameColumnWidth: Int = pref.get(ValueTreeTable.NAME_COLUMN_WIDTH, "100").toInt
  private def setDefaultNameColumnWidth(w: Int): Unit = pref.put(ValueTreeTable.NAME_COLUMN_WIDTH, w.toString)
  private def nameColumnWidth: Int = {
    if (model.getColumnCount < 1) defaultNameColumnWidth
    else {
      model.getColumn(0).getWidth
    }
  }

  // Value columns are more complicated: we keep a map of all the values seen for different amount of columns, so that
  // changing selections doesn't cause the columns to jump around all over the place. When we create a new selection for
  // the first time, we subdivide equally the total width of the current subdivision.
  //
  // We use a pref for the default width for the first set of columns / minimum size for new columns subdivisions so that
  // we don't have weirdly small columns.
  private def defaultWidthForValueCols = pref.get(ValueTreeTable.VALUE_TABLE_WIDTH, "650").toInt
  private val minSizeColRescale = 20
  private def model = dataTable.getColumnModel

  private def valueColumnWidths: Seq[Int] = {
    val n = model.getColumnCount - 1
    (0 until n).map(icol => model.getColumn(icol + 1).getWidth)
  }

  // We keep track of the sizes of all the columns for all the number of value columns we have seen.
  private var sizesSeenSoFar = Map(0 -> Seq.empty, 1 -> Seq(defaultWidthForValueCols))

  private def updateSizes(): Unit = {
    // We only do those updates when the user changes the size
    setDefaultNameColumnWidth(nameColumnWidth)
    val valuesWidths = valueColumnWidths
    if (valuesWidths.nonEmpty) {
      sizesSeenSoFar += (valueColumnWidths.size -> valuesWidths)
    }
  }

  // Creates the columns for this table.
  //
  // This table always has the following layout:
  //    | name | value 1 | value 2 | etc.
  //
  // This is enforced by this function which should always be used to create or recreate the columns.
  private def getOrCreateColumns(numOfValueCols: Int): ArrayBuffer[TableColumn[ValueViewRow]] = {
    val title: Int => String = if (numOfValueCols < 2) _ => "Value" else i => s"Value ${i + 1}"
    val nameCol = makeNameColumn(nameColumnWidth)
    val currentWidths = valueColumnWidths

    val newWidths = {
      // make sure we have up to date size data. This is important especially if the num of columns doesnt change:
      updateSizes()

      // we find if we have seen this size before. Note that we just udp
      sizesSeenSoFar.get(numOfValueCols) match {
        case Some(hasBeforeWidths) => hasBeforeWidths
        case None =>
          val toDivide = currentWidths.sum.min(defaultWidthForValueCols)
          // If its the first time we have this many columns, we just simply divide our current width down to that
          // amount. Note that numOfValueCols = 0 is added when we create this object so no divide by zero here.
          val newWidth = (toDivide / numOfValueCols).max(minSizeColRescale)
          Seq.fill(numOfValueCols)(newWidth)
      }
    }

    (Seq(nameCol) ++ newWidths.zipWithIndex.map { case (width, i) =>
      new ValueTreeTable.ValueTableColumn(title(i), i, width)
    }).to(ArrayBuffer)
  }

  // Initialization code
  locally {
    prototypesColumns = getOrCreateColumns(0)
    sumTable.showTypeAsSummary = true

    val menu = new JPopupMenu2
    menu.addCheckBoxMenu("Show Raw Values", "Don't use custom expanders", showRawValues, refreshDisplayedFields)
    menu.addCheckBoxMenu("Show Internal Values", "Show fields in optimus._", showInternalValues, refreshDisplayedFields)
    menu.addCheckBoxMenu("Show Raw Names", "Don't remove $", showRawNames, refreshDisplayedFields)

    dataTable.setComponentPopupMenu(menu)

    menu.addSeparator()
    menu.addMenu("Print Type Source") {
      val sel = getSelection
      if (sel ne null) {
        val value = sel.values(0).asInstanceOf[AnyRef]
        if (value ne null)
          println("" + value.getClass + "(" + SourceLocator.sourceOf(value.getClass) + ")")
      }
    }

    val mi = menu.addMenu("Print Stack Trace") {
      val sel = getSelection
      if (sel ne null) {
        val value = sel.values(0).asInstanceOf[AnyRef]
        value match {
          case e: Exception => e.printStackTrace()
          case _            =>
        }
      }
    }

    menu.addOnPopup {
      val sel = getSelection
      if (sel ne null) {
        val value = sel.values(0).asInstanceOf[AnyRef]
        mi.setEnabled(value.isInstanceOf[Exception])
      }
    }

    menu.addAdvMenu("Call DebuggerUI.debugSink") {
      val sel = getSelection
      if (sel ne null) {
        val value1 = sel.values(0).asInstanceOf[AnyRef]
        val value2 = sel.values(1).asInstanceOf[AnyRef]
        DebuggerUI.debugSink(value1, value2)
      }
    }

    menu.addSeparator()
    menu.addMenu("Report Constructor Calls During Entity Creation") {
      val sel = getSelection
      if (sel ne null) {
        val value1 = sel.values(0).asInstanceOf[AnyRef]
        val value2 = sel.values(1).asInstanceOf[AnyRef]
        val valueCls = if (value1 ne null) value1.getClass else value2.getClass

        val from: InstrumentationConfig.MethodRef = InstrumentationConfig.asMethodRef(valueCls.getName + ".<init>")
        val to = InstrumentationConfig.dumpIfEntityConstructing
        InstrumentationConfig.addPrefixCall(from, to, false, false)
        EntityAgent.retransform(valueCls)
        println("prefix> " + from + " " + to)
      }
    }
  }

  override def wantSummary = true
  private var _root: ValueViewRow = _
  def root: ValueViewRow = _root

  /** called when Show Internal Values etc changes to re-expand the fields based on new config */
  // noinspection ScalaUnusedSymbol
  private def refreshDisplayedFields(isSelected: Boolean): Unit =
    if (_root ne null) {
      val currentRowStates = _root.children.map(x => x.rowID -> RowState(x.open, x.diffs)).toMap
      _root.refreshChildren(currentRowStates)
      setList(Array(_root))
    }

  def inspect(values_suggested: Array[Any]): Unit = {
    // Probably don't want to compare more than MAX_TO_COMPARE?
    val values = values_suggested.take(MAX_TO_COMPARE)
    setView(getOrCreateColumns(values.length))
    updateSizes()

    val rowID =
      if (values.length == 0) RowID(title)
      else
        values.head match {
          case ss: ScenarioStack => RowID(ss) // Consider generalizing...
          case _                 => RowID(title)
        }
    _root = new ValueViewRow(rowID, values, SelectionFlags.NotFlagged, 0)
    _root.open = true // Open 1 level always
    setList(Array(_root))
  }
}
