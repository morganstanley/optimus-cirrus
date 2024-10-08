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
package optimus.profiler.ui.controls

import optimus.platform.util.html.HtmlBuilder
import optimus.platform.util.html.Link
import optimus.profiler.ui.MaxCharUtils.fullyExpandCommand
import optimus.profiler.ui.MaxCharUtils.shortenText
import optimus.profiler.ui.SClipboard
import optimus.profiler.ui.common.HasMessageBanners
import optimus.profiler.ui.common.JPopupMenu2

import javax.swing.JScrollPane
import javax.swing.JTextPane
import javax.swing.event.HyperlinkEvent.EventType
import javax.swing.text.DefaultStyledDocument
import javax.swing.text.Document
import javax.swing.text.EditorKit
import javax.swing.text.StyleConstants
import javax.swing.text.html.HTML
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit
import javax.swing.text.html.StyleSheet

/**
 * Extends default JTextPane with adding scrolling, message banners, NewLine preserving html editor Default link handler
 * to copy to clipboard and support for shortened text
 */
class JTextPaneScrollable extends JScrollPane {
  private val outpane = new JTextPaneEx()
  private var linkHandlers = Map.empty[String, () => Unit] // we aggregate handlers for unique urls in GUI?
  private var html: HtmlBuilder = new HtmlBuilder()

  setBorder(null)
  setViewportView(outpane)

  /** Rare case where currently custom entity viewer is inserted in a really strange way! */
  def getTextPane: JTextPaneEx = outpane

  /** Set editor to a given html upto the number of chars, but allows for expansion */
  def setText(hb: HtmlBuilder): Unit = setText(hb, hb.cfg.maxChars)
  def setText(hb: HtmlBuilder, maxChars: Int): Unit = {
    val shortened = shortenText(hb, maxChars)
    this.html = hb
    outpane.setText(shortened)
    outpane.setCaretPosition(0)

    // the following links were added to the HTML builder with custom handlers
    linkHandlers = hb.links.collect { case Link(url, _, Some(handler)) =>
      url -> handler
    }.toMap
  }

  def addLinkHandler(cmd: String)(handler: => Unit): Unit = {
    linkHandlers += cmd -> { () => handler }
  }

  class JTextPaneEx extends JTextPane with HasMessageBanners {
    override def createDefaultEditorKit: EditorKit = new NewLinePreservingEditorKit

    init()

    private def init(): Unit = {
      setComponentPopupMenu(createMenu())
      setAutoscrolls(true)
      setEditable(false)
      setContentType("text/html")
      addHyperlinkListener(e =>
        if (e.getEventType == EventType.ACTIVATED) {
          if (fullyExpandCommand.equals(e.getDescription)) {
            this.setText(html.toStringWithLineBreaks)
          } else
            linkHandlers.get(e.getDescription) match {
              case Some(handler) if e.getDescription.startsWith("copy_") =>
                handler()
                showMessage("Copied to Clipboard")
              case Some(handler) => handler()
              case None =>
                val preserveLineBreaks = e.getDescription.replace("<br>", "\n")
                SClipboard.copy("<i>" + preserveLineBreaks + "</i>", preserveLineBreaks)
                showMessage("Copied to Clipboard")
            }
        })
    }

    private def createMenu(): JPopupMenu2 = {
      val menu = new JPopupMenu2
      menu.addMenu("Copy All") {
        SClipboard.copy(html.toString, html.toPlaintext)
        showMessage("Copied to Clipboard")
      }
      menu
    }
  }
}

class NewLinePreservingEditorKit extends HTMLEditorKit {
  override def createDefaultDocument(): Document = {
    val ss = new StyleSheet()
    ss.addStyleSheet(getStyleSheet)

    val doc = new HTMLDocument(ss) {
      override def create(data: Array[DefaultStyledDocument.ElementSpec]): Unit = {
        data.filter(_.getAttributes ne null).foreach { spec =>
          spec.getAttributes.getAttribute(StyleConstants.NameAttribute) match {
            case HTML.Tag.BR =>
              // by default, <br> tags are interpreted as spaces
              // this changes their interpretation to \n
              assert(spec.getArray.length == 1, spec)
              spec.getArray.update(0, '\n')
            case _ =>
          }
        }
        super.create(data)
      }
      // worth considering to override insert and some other methods too
    }
    doc.setParser(getParser)
    doc.setAsynchronousLoadPriority(4)
    doc.setTokenThreshold(100)
    doc
  }
}
