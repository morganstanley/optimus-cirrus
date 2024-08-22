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

import java.awt.Dimension

import javax.swing.Box
import javax.swing.BoxLayout
import javax.swing.JButton
import javax.swing.JDialog
import javax.swing.JPanel
import javax.swing.JTextField

final class FileChooserDialog extends JDialog {

  def setDialogSize(width: Int, height: Int): Unit = {
    val dimension = new Dimension(width, height)
    setPreferredSize(dimension)
    setMinimumSize(dimension)
    setMaximumSize(dimension)
  }

  setTitle("Choose files for comparison")
  val fileChooserPanel = new FileChooserPanel(this)
  add(fileChooserPanel)

  setDialogSize(775, 130)
  setLocationRelativeTo(null)
  setVisible(true)
}

final class FileSelectionBar(buttonLabel: String, val buttonId: Int, val fileChooserDialog: FileChooserDialog)
    extends JPanel {
  setLayout(new BoxLayout(this, BoxLayout.X_AXIS))
  setPanelSize(750, 20)
  setVisible(true)

  val filePathField = new JTextField("")
  filePathField.setEditable(true)
  add(filePathField)

  val browseFileButton = new JButton(buttonLabel)
  browseFileButton.addActionListener(_ => {
    val filePaths = NodeProfiler.loadFilePaths()
    filePathField.setText(filePaths.mkString(", "))
    fileChooserDialog.setVisible(true)
  })
  add(browseFileButton)

  def setPanelSize(width: Int, height: Int): Unit = {
    val dimension = new Dimension(width, height)
    setPreferredSize(dimension)
    setMinimumSize(dimension)
    setMaximumSize(dimension)
  }
}

final class FileChooserPanel(val fileChooserDialog: FileChooserDialog) extends JPanel {
  val fileSelectionBars = List(
    new FileSelectionBar("Group 1", 1, fileChooserDialog),
    new FileSelectionBar("Group 2", 2, fileChooserDialog)
  )

  init()
  def init(): Unit = {
    setLayout(new BoxLayout(this, BoxLayout.Y_AXIS))

    addSpaceToPanelY(10)
    fileSelectionBars.foreach(bar => {
      add(bar)
      addSpaceToPanelY(3)
    })

    addSpaceToPanelY(12)
    add(new ResultTableCreator(this, fileChooserDialog))
    setVisible(true)
  }

  def addSpaceToPanelY(height: Int): Unit = add(Box.createRigidArea(new Dimension(0, height)))
}
