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

import java.awt.BorderLayout
import java.util.regex.Pattern
import javax.swing.BoxLayout
import javax.swing.JButton
import javax.swing.JCheckBox
import javax.swing.JOptionPane
import javax.swing.JPanel
import javax.swing.JTabbedPane
import optimus.graph.diagnostics.pgo.CacheFilterTweakUsageDecision.PGODiff
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.graph.diagnostics.pgo.AutoPGOThresholds
import optimus.graph.diagnostics.pgo.CacheFilterTweakUsageDecision
import optimus.graph.diagnostics.pgo.DecisionDifference
import optimus.graph.diagnostics.pgo.PNTIStats
import optimus.profiler.MergeTraces
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JToolBar2
import optimus.profiler.ui.common.JUIUtils

import java.lang.StringBuilder
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

final class CompareOgtracePanel(pgo: PGODiff, filePaths: ArrayBuffer[ArrayBuffer[String]])
    extends JPanel2(new BorderLayout(5, 5)) {
  private def init(): Unit = {
    val scopeTable = new ComparisonPanel(pgo, filePaths)
    add(scopeTable)
  }
  init()
}

object OgtraceComparisonLabels {
  val Extra = "Extra"
  val Missing = "Missing"
  val ExtraDisabled = "ExtraDisabled"
  val MissingDisabled = "MissingDisabled"

  val tabLabels = List(Extra, Missing, ExtraDisabled, MissingDisabled)
  val titleToToolTip: mutable.Map[String, String] = mutable.Map(
    (
      Extra,
      "NodeProperties that appear in Group 2 but are not present in Group 1 " +
        "(\"extra\" nodes in Group 2 relative to Group 1)"),
    (
      Missing,
      "NodeProperties that appear in Group 1 but are not present in Group 2 " +
        "(\"missing\" nodes from Group 2 relative to Group 1)"),
    (
      ExtraDisabled,
      "NodeProperties that appear in both groups with caching disabled in " +
        "Group 2, but not in Group 1 (\"extra\" caching disabled in Group 2 relative to Group 1)"),
    (
      MissingDisabled,
      "NodeProperties that appear in both groups with caching disabled in " +
        "Group 1, but not in Group 2 (\"missing\" caching disabled in Group 2 relative to Group 1)")
  )
  val titleToIndex: mutable.Map[String, Int] = mutable.Map(
    (Extra, 0),
    (Missing, 1),
    (ExtraDisabled, 2),
    (MissingDisabled, 3)
  )

}

final class ComparisonResultTabbedPane extends JTabbedPane {
  // using this kind of map so the order of the tabs stays constant
  private val resultPanels: mutable.Map[String, JPanel2] = mutable.LinkedHashMap()
  private var isRun1: Boolean = false

  // intializing the two DecisionDifferenceTables here instead of on the fly because the program needs explicit
  // access to it to add/remove the comparison data columns
  private val extraDisabled: DecisionDifferenceTable = new DecisionDifferenceTable
  private val missingDisabled: DecisionDifferenceTable = new DecisionDifferenceTable
  val decisionDifferenceTables = List(extraDisabled, missingDisabled)

  private def init(): Unit = {
    OgtraceComparisonLabels.tabLabels.foreach(o => resultPanels += (o -> new JPanel2(new BorderLayout(5, 5))))

    // adding each tab to the tabbed pane
    resultPanels foreach { case (title, tab) => add(title, tab) }
    resultPanels foreach { case (title, tab) =>
      title match {
        case OgtraceComparisonLabels.Extra | OgtraceComparisonLabels.Missing => tab.add(new PNodeDiffTable)
        case OgtraceComparisonLabels.ExtraDisabled   => extraDisabled.setViewColumns(); tab.add(extraDisabled)
        case OgtraceComparisonLabels.MissingDisabled => missingDisabled.setViewColumns(); tab.add(missingDisabled)
      }
    }
  }
  init()

  def changeRun(): Unit = isRun1 = !isRun1

  private def generatePNodeDiffTableRows(pntis: Seq[PNodeTaskInfo]): ArrayBuffer[PNodeTaskInfo] = ArrayBuffer(pntis: _*)

  private def generateDecisionDiffTableRows(
      decisionDiffs: Seq[DecisionDifference]): (ArrayBuffer[PNTIStats], ArrayBuffer[PNTIStats]) =
    (ArrayBuffer(decisionDiffs.map(_.getRun(1)): _*), ArrayBuffer(decisionDiffs.map(_.getRun(2)): _*))

  def setResultTableRows(pgo: PGODiff): Unit = {
    resultPanels.foreach { case (title, tab) =>
      tab.getComponents.foreach {
        case pNodeDiffTable: PNodeDiffTable =>
          pNodeDiffTable.setViewColumns()
          val rowsMissing: ArrayBuffer[PNodeTaskInfo] = generatePNodeDiffTableRows(pgo.missing)
          val rowsExtra: ArrayBuffer[PNodeTaskInfo] = generatePNodeDiffTableRows(pgo.extra)
          val rowsEmpty: ArrayBuffer[PNodeTaskInfo] = ArrayBuffer.empty[PNodeTaskInfo]
          title match {
            case OgtraceComparisonLabels.Extra =>
              if (isRun1) pNodeDiffTable.setRows(rowsMissing, rowsEmpty)
              else pNodeDiffTable.setRows(rowsExtra, rowsEmpty)
            case OgtraceComparisonLabels.Missing =>
              if (isRun1) pNodeDiffTable.setRows(rowsExtra, rowsEmpty)
              else pNodeDiffTable.setRows(rowsMissing, rowsEmpty)
          }
        case decisionDiffTable: DecisionDifferenceTable =>
          val (rowsMissingDisabled, comparisonRowsMissingDisabled) = generateDecisionDiffTableRows(pgo.missingDisabled)
          val (rowsExtraDisabled, comparisonRowsExtraDisabled) = generateDecisionDiffTableRows(pgo.extraDisabled)
          title match {
            case OgtraceComparisonLabels.ExtraDisabled =>
              if (isRun1)
                decisionDiffTable.setRows(rowsMissingDisabled, comparisonRowsMissingDisabled)
              else
                decisionDiffTable.setRows(rowsExtraDisabled, comparisonRowsExtraDisabled)
            case OgtraceComparisonLabels.MissingDisabled =>
              if (isRun1)
                decisionDiffTable.setRows(rowsExtraDisabled, comparisonRowsExtraDisabled)
              else
                decisionDiffTable.setRows(rowsMissingDisabled, comparisonRowsMissingDisabled)
          }
        case _ =>
      }
    }
    setTabTooltips()
  }

  private def setTabTooltips(): Unit = {
    setToolTipTextAt(
      OgtraceComparisonLabels.titleToIndex(OgtraceComparisonLabels.Extra),
      OgtraceComparisonLabels.titleToToolTip(OgtraceComparisonLabels.Extra))
    setToolTipTextAt(
      OgtraceComparisonLabels.titleToIndex(OgtraceComparisonLabels.Missing),
      OgtraceComparisonLabels.titleToToolTip(OgtraceComparisonLabels.Missing))
    setToolTipTextAt(
      OgtraceComparisonLabels.titleToIndex(OgtraceComparisonLabels.ExtraDisabled),
      OgtraceComparisonLabels.titleToToolTip(OgtraceComparisonLabels.ExtraDisabled)
    )
    setToolTipTextAt(
      OgtraceComparisonLabels.titleToIndex(OgtraceComparisonLabels.MissingDisabled),
      OgtraceComparisonLabels.titleToToolTip(OgtraceComparisonLabels.MissingDisabled)
    )
  }
}

final class ResultTableCreator(fileChooserPanel: FileChooserPanel, fileChooserDialog: FileChooserDialog)
    extends JPanel {
  import scala.jdk.CollectionConverters._
  setLayout(new BoxLayout(this, BoxLayout.X_AXIS))

  private var pntisA: Option[Seq[PNodeTaskInfo]] = _
  private var pntisB: Option[Seq[PNodeTaskInfo]] = _
  private var pgo: CacheFilterTweakUsageDecision.PGODiff = _

  private def loadAndMergePntisFromPath(
      paths: ArrayBuffer[String],
      resultTableCreator: ResultTableCreator): Option[Seq[PNodeTaskInfo]] = {
    val files = ArraySeq.newBuilder[String]
    var pntis: Option[Seq[PNodeTaskInfo]] = None

    JUIUtils.readFromFilePaths(null, paths) { file =>
      if (file.isFile) {
        files += file.toString
      } else
        JOptionPane.showMessageDialog(
          resultTableCreator,
          s"There is an issue reading $file. Please check and try again.")
    }
    val filesRes = files.result()
    if (filesRes.nonEmpty) {
      val (tmp, _) = MergeTraces.mergeTraces(filesRes)
      pntis = Some(tmp)
    }
    pntis
  }

  private def getPNTIs(i: Int): Option[Seq[PNodeTaskInfo]] = {
    var text: String = ""
    if (i == 0 || i == 1) text = fileChooserPanel.fileSelectionBars(i).filePathField.getText()
    else throw new IllegalArgumentException("Can only pass in 0 or 1 for i")
    loadAndMergePntisFromPath(splitFilePaths(text), this)
  }

  private def getFilePathsArrayBuffer(i: Int): ArrayBuffer[String] = {
    if (i == 0) splitFilePaths(getFileBarText(fileChooserPanel.fileSelectionBars(0)))
    else if (i == 1) splitFilePaths(getFileBarText(fileChooserPanel.fileSelectionBars(1)))
    else throw new IllegalArgumentException("Can only pass in 0 or 1 for i")
  }

  init()
  private def init(): Unit = {
    val compareButton = new JButton("Compare selected files")
    compareButton.setToolTipText("If multiple files are chosen for a group, the ogtrace files will be merged together")
    compareButton.addActionListener(_ => {
      pntisA = getPNTIs(0)
      pntisB = getPNTIs(1)
      val filePaths = ArrayBuffer[ArrayBuffer[String]](getFilePathsArrayBuffer(0), getFilePathsArrayBuffer(1))

      (pntisA, pntisB) match {
        case (Some(a), Some(b)) =>
          pgo = CacheFilterTweakUsageDecision.compareRuns(a.asJava, b.asJava, threshold = 5, AutoPGOThresholds())
          GraphDebuggerUI.addTab("Compare .ogtrace files ", new CompareOgtracePanel(pgo, filePaths))
          fileChooserDialog.setVisible(false)
        case (_, _) =>
      }
    })
    add(compareButton)
  }

  private def getFileBarText(fileSelectionBar: FileSelectionBar): String = fileSelectionBar.filePathField.getText

  private def splitFilePaths(unEditedPaths: String): ArrayBuffer[String] = ArrayBuffer.from(unEditedPaths.split(","))
}

object ComparisonPanel {
  private val pattern: String = Pattern.quote(System.getProperty("file.separator"))
}

final class ComparisonPanel(pgo: PGODiff, filePaths: ArrayBuffer[ArrayBuffer[String]])
    extends JPanel2(new BorderLayout(5, 5)) {
  private val comparisonResultTabbedPane: ComparisonResultTabbedPane = new ComparisonResultTabbedPane

  private def makeToolTip: String = {
    val group1 = new StringBuilder
    val group2 = new StringBuilder
    def splitFn(s: String, group: StringBuilder) = {
      val filename = s.split(ComparisonPanel.pattern).last
      group.append(s"<li>${filename.dropRight(GridProfiler.ProfilerOutputStrings.ogtraceExtension.length + 1)}</li>")
    }
    filePaths(0).foreach(s => splitFn(s, group1))
    filePaths(1).foreach(s => splitFn(s, group2))
    s"""<html>Change view to see the results of the other group's perspective.
    Currently the groups are:<br><br> Group 1 (the reference group): <ul>${group1}</ul><br>
      Group 2: <ul>${group2}</ul></html>
      """
  }

  private def addComparisonData(): Unit =
    comparisonResultTabbedPane.decisionDifferenceTables.foreach(_.addComparisonDataColumns())

  private val toolBar = {
    val toolBar = new JToolBar2()
    makeButtons(toolBar)
    toolBar
  }

  private def makeButtons(toolBar: JToolBar2): Unit = {
    val changeButton = new JButton("Change Groups")
    changeButton.addActionListener(_ => {
      comparisonResultTabbedPane.changeRun()
      comparisonResultTabbedPane.setResultTableRows(pgo)
      val tmp: ArrayBuffer[String] = filePaths(0).clone()
      filePaths(0) = filePaths(1)
      filePaths(1) = tmp
      changeButton.setToolTipText(makeToolTip)
    })
    changeButton.setToolTipText(makeToolTip)

    val showGroup2Data = new JCheckBox("Show Comparison Data")
    showGroup2Data.setSelected(true)
    addComparisonData()
    showGroup2Data.addActionListener(action => {
      val checkBox: JCheckBox = action.getSource.asInstanceOf[JCheckBox]
      if (checkBox.isSelected) addComparisonData()
      else comparisonResultTabbedPane.decisionDifferenceTables.foreach(_.removeComparisonDataColumnsFromView())
    })
    toolBar.add(changeButton)
    toolBar.add(showGroup2Data)
  }

  private def init(): Unit = {
    comparisonResultTabbedPane.setResultTableRows(pgo)
    comparisonResultTabbedPane.setVisible(true)
    add(toolBar, BorderLayout.NORTH)
    add(comparisonResultTabbedPane, BorderLayout.CENTER)
  }
  init()
}
