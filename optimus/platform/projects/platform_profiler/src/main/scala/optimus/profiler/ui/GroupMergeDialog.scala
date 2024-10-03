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

import java.awt.Component
import java.awt.Container
import java.awt.GridBagConstraints
import java.awt.GridBagLayout
import java.awt.Insets

import javax.swing.JCheckBox
import javax.swing.JComponent
import javax.swing.JLabel
import javax.swing.JOptionPane
import javax.swing.JPanel
import javax.swing.JScrollPane
import javax.swing.JTextArea
import javax.swing.JTextField
import javax.swing.ScrollPaneConstants
import javax.swing.UIManager
import optimus.profiler.MergeTraces

class GroupMergeDialog {
  private val withDisableXSFTCheckBox: JCheckBox = new JCheckBox("WithDisableXSFT")
  withDisableXSFTCheckBox.setToolTipText("Disable XSFT")

  private val resourceTextField: JTextField = new JTextField(50)
  private val artifactsTextField: JTextField = new JTextField(50)
  resourceTextField.setToolTipText("Path to artifacts")
  artifactsTextField.setToolTipText("Regression test run unique number")

  private val groupNameTextArea: JTextArea = new JTextArea(7, 50)
  private val groupNameScrollPane: JScrollPane = setScrollableTextArea(groupNameTextArea)
  groupNameTextArea.setToolTipText("Group name encompassing one or multiple modules")

  private val PGOJobPanel = new JPanel()
  private val insets = new Insets(5, 3, 5, 3)
  PGOJobPanel.setLayout(new GridBagLayout())

  setPanel(
    (new JLabel("Resource:"), resourceTextField),
    (new JLabel("Build Artifacts:"), artifactsTextField),
    (new JLabel("Group names:"), groupNameScrollPane)
  )
  addComponent(PGOJobPanel, withDisableXSFTCheckBox, 0, 4)

  def groupTracesInput(): (Option[String], Option[String], Option[Seq[String]], Option[Seq[String]]) = {
    val result: Int =
      JOptionPane.showConfirmDialog(null, PGOJobPanel, "Please Insert", JOptionPane.OK_CANCEL_OPTION)

    if (result == JOptionPane.OK_OPTION) {
      val rootDir =
        if (!resourceTextField.getText.trim.isEmpty)
          Some(resourceTextField.getText.trim)
        else {
          None
        }

      val artifactsPath =
        if (!artifactsTextField.getText.trim.isEmpty)
          Some(artifactsTextField.getText.trim)
        else {
          None
        }

      val names =
        if (!groupNameTextArea.getText.trim.isEmpty)
          Some(MergeTraces.convert(groupNameTextArea.getText.trim))
        else
          Some(Seq.empty[String])

      val withDisableXSFT =
        if (withDisableXSFTCheckBox.isSelected) names
        else
          Some(Seq.empty[String])

      (rootDir, artifactsPath, names, withDisableXSFT)
    } else (null, null, null, null)
  }

  private def setScrollableTextArea(textArea: JTextArea): JScrollPane = {
    textArea.setFont(UIManager.getFont("Label.font"))
    textArea.setLineWrap(true)
    textArea.setWrapStyleWord(true)

    new JScrollPane(
      textArea,
      ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
      ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER)
  }

  private def setPanel(labelComponents: (JLabel, JComponent)*): Unit = {
    for ((labelComponent, gridY) <- labelComponents.zipWithIndex) {
      addComponent(PGOJobPanel, labelComponent._2, 1, gridY)
      addComponent(PGOJobPanel, labelComponent._1, 0, gridY)
    }
    PGOJobPanel.setAlignmentX(Component.LEFT_ALIGNMENT)
  }

  private def addComponent(
      container: Container,
      component: Component,
      gridX: Int,
      gridY: Int,
      gridWidth: Int = 1,
      gridHeight: Int = 1,
      anchor: Int = GridBagConstraints.LINE_START,
      fill: Int = GridBagConstraints.HORIZONTAL): Unit = {

    val gbc: GridBagConstraints =
      new GridBagConstraints(gridX, gridY, gridWidth, gridHeight, 1.0d, 1.0d, anchor, fill, insets, 0, 0)
    container.add(component, gbc)
  }
}
