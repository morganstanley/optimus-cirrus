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
package optimus.profiler.ui.common

import java.io.File
import java.util.prefs.Preferences
import javax.swing.JComponent
import javax.swing.JFileChooser
import javax.swing.JOptionPane
import javax.swing.filechooser.FileFilter
import optimus.config.NodeCacheInfo
import optimus.graph.diagnostics.gridprofiler.GridProfiler.ProfilerOutputStrings
import optimus.profiler.ProfilerUI
import msjava.slf4jutils.scalalog.getLogger
import com.github.benmanes.caffeine.cache.Caffeine
import optimus.config.NodeCacheConfigs
import optimus.core.CoreHelpers

import java.awt.BorderLayout
import java.io.{FileFilter => JFileFilter}
import javax.swing.JLabel
import javax.swing.JPanel
import javax.swing.JTextField
import javax.swing.border.EmptyBorder
import scala.collection.mutable.ArrayBuffer

object JUIUtils {
  private val log = getLogger(this)
  // note: this uses identity hash on keys
  private val weakCacheOfSlowStrings = Caffeine.newBuilder.weakKeys.build[AnyRef, String]()

  def weakCacheSlowString(s: AnyRef): String = {
    if (s eq null) "[null]"
    else {
      var text = weakCacheOfSlowStrings.getIfPresent(s)
      if (text eq null) {
        val start = System.nanoTime
        text = CoreHelpers.safeToString(s) // graph call toString! Consider underStackOf?
        val duration = System.nanoTime - start
        if (duration > 1e8) { // anything slower than 100ms ends up here
          weakCacheOfSlowStrings.put(s, text)
          log.warn(s"Slow toString on ${s.getClass}! [${"%.0f".format(duration * 1e-9)}s]")
        }
      }
      text
    }
  }

  def readFromFilePaths(parent: JComponent, paths: ArrayBuffer[String])(process: File => Unit): Unit = {
    try {
      paths.foreach(filePath => process(new File(filePath.strip())))
    } catch {
      case ex: Exception =>
        JOptionPane.showMessageDialog(parent, ex.toString, "Could not load file", JOptionPane.ERROR_MESSAGE);
    }
  }

  def readFromFile(parent: JComponent, fc: JFileChooser, multiple: Boolean = false)(process: File => Unit): Unit = {
    try {
      fc.setMultiSelectionEnabled(multiple)
      if (fc.showOpenDialog(parent) == JFileChooser.APPROVE_OPTION) {
        if (multiple) {
          fc.getSelectedFiles.foreach(file => process(file))
        } else
          process(fc.getSelectedFile)
      }
    } catch {
      case ex: Exception =>
        JOptionPane.showMessageDialog(parent, ex.toString, "Could not load file", JOptionPane.ERROR_MESSAGE);
    }
  }

  def saveToFile(parent: JComponent, fc: JFileChooser2)(process: File => Unit): Unit = {
    try {
      if (fc.showSaveDialog(parent) == JFileChooser.APPROVE_OPTION) {
        val fileSelected = fc.getSelectedFile
        val file = FileChooser.setSuffixIfNeeded(fileSelected, fc.extension)
        process(file)
      }
    } catch {
      case ex: Exception =>
        JOptionPane.showMessageDialog(parent, ex.toString, "Could not save file", JOptionPane.ERROR_MESSAGE);
    }
  }

  def createPanelForTextField(textField: JTextField, name: String): JPanel = {
    val label = new JLabel(name)
    val maxOutputPanel = new JPanel(new BorderLayout())
    label.setBorder(new EmptyBorder(0, 5, 0, 5)) // padding
    maxOutputPanel.add(label, BorderLayout.WEST)
    maxOutputPanel.add(textField, BorderLayout.CENTER)
    maxOutputPanel
  }
}

private[ui] trait ConfigSettings {
  private def prefName: String = this.getClass.getSimpleName.toLowerCase stripSuffix "$"
  private def packagePref: Preferences = Preferences.userNodeForPackage(this.getClass)
  protected val pref: Preferences = packagePref.node(prefName)
  protected def prefOf(subKey: AnyRef): Preferences = packagePref.node(prefName + subKey)
}

object FileChooser {
  def lastOGTraceDir: String = ProfilerUI.pref.get(lastDir(ogtraceSuffix), null)
  def lastDir(extension: String): String = "last.dir" + extension
  val ogtraceSuffix = s".${ProfilerOutputStrings.ogtraceExtension}"
  val nodeDumpSuffix = ".json"
  val graphProfileSuffix = ".graphprofile"
  val htmlLogSuffix = ".log.html"
  val optConfSuffix: String = NodeCacheInfo.configurationSuffix

  private def makeFilter(suffix: String, name: String) = new FileFilter with JFileFilter {
    override def accept(f: File): Boolean = f.getName.endsWith(suffix) || f.isDirectory
    override def getDescription: String = name
  }

  def ogtraceFilter: JFileFilter = makeFilter(ogtraceSuffix, "Traces")

  def setSuffixIfNeeded(file: File, suffix: String): File = {
    if (!file.getName.endsWith(suffix)) new File(file.getAbsolutePath + suffix) else file
  }

  def traceFileChooser: JFileChooser2 = mkFileChooser("Recorded Traces", ogtraceSuffix)
  def graphFileChooser: JFileChooser2 = mkFileChooser("Graph Profiles", graphProfileSuffix)
  def nodeDumpChooser: JFileChooser2 = mkFileChooser("Node Dumps", nodeDumpSuffix)
  def htmlLogFileChooser: JFileChooser2 = mkFileChooser("Console Logs", htmlLogSuffix)
  def optConfFileChooser(option: JComponent): JFileChooser2 = {
    val optconfPaths = NodeCacheConfigs.getOptconfProviderPaths
    val optconfPath = if (optconfPaths.nonEmpty) optconfPaths.head else null
    mkFileChooser("Optimus Configs", optConfSuffix, optconfPath, option)
  }

  private def mkFileChooser(
      desc: String,
      ext: String,
      defFile: String = null,
      options: JComponent = null): JFileChooser2 = {
    val filter = makeFilter(ext, desc)
    val key = lastDir(ext)
    val fc = new JFileChooser2(key, ext)
    if (options ne null) fc.setAccessory(options)
    fc.setFileFilter(filter)

    val file =
      if (defFile ne null) new File(defFile)
      else new File(ProfilerUI.pref.get(key, new File(System.getProperty("user.home"), "1" + ext).getAbsolutePath))
    fc.setSelectedFile(file)
    fc
  }
}

class JFileChooser2(key: String, val extension: String) extends JFileChooser {
  override def approveSelection(): Unit = {
    super.approveSelection()
    ProfilerUI.pref.put(key, getSelectedFile.getAbsolutePath)
    val extension = getAccessory.asInstanceOf[StoreSettings] // save configured options (e.g. for generating optconf)
    if (extension ne null) extension.store()
  }
}

/** This allow for customized FileChoosers (via  accessory/option) to let it store additional settings */
trait StoreSettings {
  def store(): Unit
}
