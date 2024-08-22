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

import java.{util => ju}
import java.awt.BorderLayout
import java.awt.Desktop
import java.awt.Dimension
import java.awt.event.WindowEvent
import java.net.URI

import javax.swing.JButton
import javax.swing.JDialog
import javax.swing.JFrame
import javax.swing.JScrollPane
import javax.swing.JTextPane
import javax.swing.WindowConstants
import javax.swing.event.HyperlinkEvent.EventType
import optimus.profiler.ui.common.ConfigSettings
import optimus.profiler.ui.common.HasMessageBanners
import org.yaml.snakeyaml.Yaml

import scala.jdk.CollectionConverters._
import scala.xml.Unparsed

object WhatsNew extends ConfigSettings {
  final case class Update(versionTitle: String, features: List[String], bugfixes: List[String])
  private val updates: List[Update] = {
    implicit class YamlOps(yaml: Any) {
      def get(key: String): Any = if (yaml == null) null else yaml.asInstanceOf[ju.Map[String, Any]].get(key)
      def toList: List[Any] = if (yaml == null) Nil else yaml.asInstanceOf[ju.List[Any]].asScala.toList
      def toListOf[T]: List[T] = toList.asInstanceOf[List[T]]
    }
    val yaml = new Yaml().load[AnyRef](getClass.getResourceAsStream("news.yaml"))
    yaml.toList.map { item =>
      Update(
        versionTitle = item.get("version").asInstanceOf[String],
        features = item.get("features").toListOf[String],
        bugfixes = item.get("bugfixes").toListOf[String]
      )
    }
  }
  val currentVersion = updates.length

  final val lastVersionViewedKey = "lastVersionViewed"
  def lastVersionViewed = pref.getInt(lastVersionViewedKey, 0)
  def lastVersionViewed_=(v: Int): Unit = { pref.putInt(lastVersionViewedKey, v) }

  def show(owner: JFrame): Unit = {
    val seenVersion = if (owner == null) 0 else lastVersionViewed
    val currentVersion = updates.length
    if (seenVersion < currentVersion) {
      val whatsNew = new WhatsNew(owner, generateMessage(updates))
      whatsNew.setVisible(true)
    }
  }

  private def generateMessage(updates: List[Update]): String = {
    def mkList(title: String, items: List[String]) =
      if (items.isEmpty) Nil
      else {
        <b>{title}</b>
        <br />
        <ul>
          {items.map(item => <li>{Unparsed(item)}</li>)}
        </ul>
      }
    val html =
      <html style="font-family: consolas; font-size: 12pt">
        {
        updates.reverseMap { update =>
          <div>
              <i>{update.versionTitle}</i>
              <br />
              {mkList("New", update.features)}
              {mkList("Fixes", update.bugfixes)}
            </div>
        }
      }
      </html>
    html.toString
  }

}

class WhatsNew(owner: JFrame, msg: String) extends JDialog(owner, "What's New in Graph Debugger/Profiler") {
  private val outpane = new JTextPane() with HasMessageBanners

  init(msg)

  private def init(msg: String): Unit = {
    setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)
    setLayout(new BorderLayout(3, 3))

    outpane.setAutoscrolls(true)
    outpane.setEditable(false)
    outpane.setContentType("text/html")
    outpane.setText(msg)
    outpane.setCaretPosition(0)
    outpane.addHyperlinkListener { e =>
      if (e.getEventType == EventType.ACTIVATED) {
        if (e.getDescription.startsWith("http://"))
          Desktop.getDesktop.browse(URI.create(e.getDescription))
        else {
          SClipboard.copy("<i>" + e.getDescription + "</i>", e.getDescription)
          outpane.showMessage("Copied to Clipboard")
        }
      }
    }
    outpane.setPreferredSize(new Dimension(800, 500))

    val button = new JButton("OK")
    button.addActionListener(_ => dispose())
    add(new JScrollPane(outpane), BorderLayout.CENTER)
    add(button, BorderLayout.SOUTH)
    pack()
    setLocationRelativeTo(getParent)
  }

  override def processWindowEvent(e: WindowEvent): Unit = {
    if (e.getID == WindowEvent.WINDOW_CLOSED) {
      WhatsNew.lastVersionViewed = WhatsNew.currentVersion
    }
    super.processWindowEvent(e)
  }
}
