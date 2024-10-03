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

import optimus.platform.util.html.DetailsConfig.maxCharsMinimum
import optimus.platform.util.html.HtmlBuilder
import optimus.profiler.ui.common.JUIUtils

import javax.swing.JOptionPane
import javax.swing.JPanel
import javax.swing.JTextField
import javax.swing.ToolTipManager

private[ui] object MaxCharUtils {
  val name = "Max chars"
  val fullyExpandCommand = "fullyExpand()"

  ToolTipManager.sharedInstance().setDismissDelay(10000)

  def createMaxOutputPanel(maxOutputField: JTextField): JPanel = JUIUtils.createPanelForTextField(maxOutputField, name)

  def createMaxOutputField(maxChars: Int = maxCharsMinimum): JTextField = {
    val maxOutputField = new JTextField(maxChars.toString)
    maxOutputField.setToolTipText(
      s"<html>Max characters to display in output pane (+ve integer or 'max')" +
        "<br>Note: the text displayed is wrapped in HTML to control formatting, " +
        "<br>so you might see fewer actual characters " +
        s"than the number you enter here. " +
        s"<br>We always display at least $maxCharsMinimum characters.</html>"
    )
    maxOutputField
  }

  def shortenTextHelper(text: String, maxChar: Int): String = {
    val shortened = text.substring(0, maxChar)
    val lastLt = shortened.lastIndexOf('<')
    val lastGt = shortened.lastIndexOf('>')
    if (lastGt < lastLt)
      shortened.substring(0, lastLt)
    else
      shortened
  }

  /** Returns formatted full string and shortened string */
  def shortenText(hb: HtmlBuilder, maxChars: Int): String = {
    // Because some of our char count is eaten up by HTML wrapping, we set minimum to 1000 to avoid confusing people
    val fullString = hb.toStringWithLineBreaks
    val abbreviationLink =
      "<br><br><a href='" + fullyExpandCommand + "'>[" + (fullString.length - maxChars) + " truncated. click to expand]</a>"

    val actualMax = if (maxChars < maxCharsMinimum) maxCharsMinimum else maxChars
    if (fullString.length > actualMax) {
      shortenTextHelper(fullString, maxChars) + abbreviationLink
    } else fullString
  }

  private def errorDialog(): Unit =
    JOptionPane.showMessageDialog(
      null,
      s"Only integer > $maxCharsMinimum or 'max' is acceptable",
      "Invalid Input",
      JOptionPane.WARNING_MESSAGE)

  def readMaxCharsFrom(maxOutputField: JTextField): Int = {
    val input = maxOutputField.getText
    input match {
      case s: String =>
        if (s.toLowerCase == "max") Integer.MAX_VALUE
        else {
          try {
            val userInput = Integer.parseInt(s)
            if (userInput < maxCharsMinimum) {
              errorDialog()
              maxCharsMinimum
            } else userInput
          } catch {
            case _: Exception =>
              errorDialog()
              maxCharsMinimum
          }
        }
      case _ => maxCharsMinimum
    }
  }
}
