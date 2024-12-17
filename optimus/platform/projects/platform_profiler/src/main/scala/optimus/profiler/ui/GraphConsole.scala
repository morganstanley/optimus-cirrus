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

import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.DbgPreference
import optimus.platform._
import optimus.platform.utils.ClassPathUtils
import optimus.profiler.DebuggerUI
import optimus.profiler.DebuggerUI.xval_any
import optimus.profiler.recipes.Recipes
import optimus.profiler.ui.common.FileChooser
import optimus.profiler.ui.common.JPanel2
import optimus.profiler.ui.common.JPopupMenu2
import optimus.profiler.ui.common.JToolBar2
import optimus.profiler.ui.common.JUIUtils
import optimus.scalacompat.repl.Completions
import optimus.scalacompat.repl.ScalaInterpreter
import optimus.scalacompat.repl.ScalaInterpreterSettings

import java.awt.BorderLayout
import java.awt.Color
import java.awt.Dimension
import java.awt.EventQueue
import java.awt.Font
import java.awt.event.ActionEvent
import java.awt.event.KeyAdapter
import java.awt.event.KeyEvent
import java.awt.event.MouseEvent
import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.io.PrintWriter
import java.io.Writer
import javax.swing.JButton
import javax.swing.JLabel
import javax.swing.JOptionPane
import javax.swing.JPanel
import javax.swing.JScrollPane
import javax.swing.JTextField
import javax.swing.JTextPane
import javax.swing.border.EmptyBorder
import javax.swing.text.html.HTMLDocument
import scala.reflect.runtime.{universe => ru}
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Results

object GraphConsole {
  val autoSave: DbgPreference = DbgPreference("Auto Save", default = true)
  val truncate: DbgPreference = DbgPreference("Truncate Results", default = true)

  private val startStyle = "<style type='text/css'>" +
    s"body { font-family: Menlo, Monaco, Consolas, 'Courier New', monospace; font-size: ${Math.round(12 * Fonts.multiplier)}pt; } " +
    ".in { } .cand { color: blue; } " +
    ".out { margin-left: 10; font-style: italic; background-color: #fdfdd3; } " +
    "</style>"

  private val startBanner =
    "Available actions: <b>brk</b>, <b>out()</b>, <b>browse()</b>, <b>inspect()</b><br>" +
      "Same functions are available in your code as <b>Debugger.xxx</b><br><br>"

  private var panel: GraphConsole = _
  def instance: GraphConsole = {
    if (panel eq null) NodeUI.invoke {
      if (panel eq null) panel = new GraphConsole()
    }
    panel
  }

  def out(str: String): Unit = if (NodeUI.isInitialized) instance.write(str)

  def appendHtml(html: String): Unit = if (NodeUI.isInitialized) instance.appendHtml(html)

  def execute(str: String): Unit = if (NodeUI.isInitialized) instance.execute(str)

  def prompt(str: String, pos: Int, posEnd: Int): Unit = if (NodeUI.isInitialized) {
    instance
    EventQueue.invokeLater(() => {
      panel.in.requestFocus()
      panel.in.setText(str)
      panel.in.setSelectionStart(pos)
      panel.in.setSelectionEnd(posEnd)
    })
  }

  private[ui] def createInterpreter(buffer: Writer, executeFirst: String) = {
    ScalaInterpreter(new ScalaInterpreterSettings {
      override def classpath: String = ClassPathUtils.expandedApplicationClasspathString
      override def useJavaClasspath = true
      override def out = new PrintWriter(buffer)
      override def parentClassLoader: Option[ClassLoader] = Some(DebuggerUI.getClass.getClassLoader)
      override def seedScript: String = executeFirst
      override def changeLineName = true
      override def maxPrintString: Option[Int] = Some(1000)
      override def echoInit = false
      override def scalaSettings(settings: Settings): Unit = {
        super.scalaSettings(settings)
        settings.YaliasPackage.tryToSet(PackageAliases.aliases)
      }
    })
  }

  private[ui] def interpret(intp: ScalaInterpreter, line: String): Results.Result = {
    intp.interpret(line, GraphConsole.truncate.get)
  }

  private val StartExcludes = "^(Abstract|StrictOptimized|Has).*".r
  private val EndExcludes = ".*(Ops|Like|Template|Optimized|Defaults|Parallelizable|Serializable)$" r
  def getType[T](jclasz: Class[T]): String = {
    val iu = ru.asInstanceOf[reflect.runtime.JavaUniverse]; import iu._
    def isUnprincipledBaseClass(sym: ClassSymbol): Boolean = {
      val name = sym.name.toString
      name == "Serializable" ||
      sym.enclosingPackage.fullNameString.startsWith("scala.collection") && {
        StartExcludes.pattern.matcher(name).matches || EndExcludes.pattern.matcher(name).matches
      }
    }
    try {
      if (jclasz.isHidden)
        return getType(jclasz.getSuperclass)
      // the hope is to produce a string which can be compiled by REPL into a useful type for a value with class `jclasz`.
      val clasz = iu runtimeMirror jclasz.getClassLoader classSymbol jclasz
      val tpe = clasz.baseClasses.collectFirst {
        case base: ClassSymbol
            if ( // find the first base class instance which
              base.owner.hasPackageFlag // - is owned by a package (so no Foo$Bar names)
                && !base.isModuleClass // - isn't itself a module (so no scala.None$)
                && !(base.isPrivate || base.isProtected) // - is likely to be visible to us
                && !isUnprincipledBaseClass(base) // - isn't unuseful for various reasons
            ) => //
          clasz.tpe.erasure baseType base // and resolve it as the actual base type
        // (this means that nodes are typed as PropertyNode[Whatever], for instance)
      }.get
      def loop(tpe: Type): String = tpe match {
        case TypeRef(_, sym, args) =>
          sym.fullNameString + (if (args.isEmpty) "" else args.map(loop).mkString("[", ",", "]"))
        case SingleType(_, sym) => sym.fullNameString
        case ExistentialType(quants, ul) =>
          loop(ul.map {
            case q if quants contains q.typeSymbol => q.typeSymbol.info.upperBound
            case t                                 => t
          })
      }
      loop(tpe)
    } catch {
      case _: Throwable => jclasz.getName
    }
  }

  private[ui] def prepareDump(xval: Object, name: String, cnt: Int): String = {
    val sig = GraphConsole.getType(xval.getClass)
    if (sig.nonEmpty)
      s"val $name$cnt = xval_any.asInstanceOf[$sig]"
    else
      s"val $name$cnt = xval_any"
  }

  private[ui] def prepareResultLambda(tpe: Class[_ <: AnyRef], expr: String): String =
    s"""
       |optimus.profiler.DebuggerUI.currResultLambda = x => {
       |  val t = x.asInstanceOf[${tpe.getName}]
       |  $expr
       |}
       |""".stripMargin // [SEE_SET_CURR_RES_LAMBDA]
}

class GraphConsole private extends JPanel2 {
  private var in: JTextField = _

  private val inStyle: String = "in"
  private val outStyle: String = "out"
  private val candidatesStyle: String = "cand"

  private val title = new JLabel("Console:")
  private var outpane: JTextPane = _
  private var out: HTMLDocument = _
  private var bodyElm: HTMLDocument#BlockElement = _
  private def resetBodyElm(): Unit = {
    out = outpane.getStyledDocument.asInstanceOf[HTMLDocument]
    val rootElm = out.getDefaultRootElement
    bodyElm = rootElm.getElement(1).asInstanceOf[HTMLDocument#BlockElement]
    if (bodyElm eq null) {
      out.insertAfterStart(rootElm, GraphConsole.startStyle)
      bodyElm = rootElm.getElement(0).asInstanceOf[HTMLDocument#BlockElement]
    }
  }

  private var lastSetOfCandidates: Completions = Completions.NoCompletions

  def appendHtml(html: String): Unit = out.insertBeforeEnd(bodyElm, html)

  private object outBuffer extends Writer {
    override def write(cbuf: Array[Char], off: Int, len: Int): Unit = {
      val uselen = if (len > 1 && cbuf(len - 1) == '\n') len - 1 else len
      write(new String(cbuf, off, uselen), outStyle)
    }
    override def write(str: String): Unit = write(str, outStyle)

    def write(str: String, style: String): Unit = {
      if (out ne null) {
        if (!EventQueue.isDispatchThread) EventQueue.invokeLater(() => write(str, style))
        else {
          if (str == "\r")
            out.insertBeforeEnd(bodyElm, "<br>")
          else {
            val lines = str.split('\n')
            lines.foreach { line =>
              out.insertBeforeEnd(bodyElm, "<div class=" + style + "></div>")
              val cc = bodyElm.getChildCount
              val newElm = bodyElm.getElement(cc - 1)
              out.insertString(newElm.getStartOffset, line, null)
            }
          }
          outpane.setCaretPosition(out.getLength)
        }
      }
    }
    override def close(): Unit = {}
    override def flush(): Unit = {}
  }

  def write(str: String): Unit = outBuffer.write(str)
  def writeInput(str: String): Unit = outBuffer.write(str, inStyle)
  def write(str: String, style: String): Unit = outBuffer.write(str, style)

  private lazy val interpreter = {
    GraphDebuggerUI.showMessage("Initializing REPL...")
    GraphConsole.createInterpreter(outBuffer, Recipes.defaultImport)
  }

  init()

  private def autoComplete(silent: Boolean): Unit = {
    lastSetOfCandidates = Completions.NoCompletions
    val curPos = in.getCaretPosition
    val text = in.getText()
    val candidates = interpreter.complete(text, curPos)
    val lst = candidates.candidates
    if (lst.nonEmpty) {
      val single = lst.tail.isEmpty
      val preselect = lst.head
      val doc = in.getDocument
      val add = preselect.substring(curPos - candidates.cursor)
      doc.insertString(curPos, add, null)
      if (!single) {
        lastSetOfCandidates = candidates
        in.setSelectionStart(curPos)
        if (!silent) {
          val output = candidates.candidates.mkString("\n", " ", "\n")
          write(output, candidatesStyle)
        }
      }
    }
  }

  /**
   * Dump obj... and give it a name
   */
  private var cnt = 0
  def dump(o: Any, name: String = "x"): Unit = {
    xval_any = o.asInstanceOf[Object]
    if (xval_any ne null) {
      GraphDebuggerUI.toFront()
      val line = GraphConsole.prepareDump(xval_any, name, cnt)
      GraphConsole.execute(line)
      cnt += 1
    } else
      GraphConsole.out("null\n")
  }

  private def execute(action: String, writeAction: Boolean = true): Unit = {
    def userAcceptsDeadlockRisk = {
      val warning = "Graph Execution is currently stopped. If you can continue you may deadlock!"
      val r = JOptionPane.showConfirmDialog(this, warning, "Would you like to continue?", JOptionPane.OK_CANCEL_OPTION)
      r == JOptionPane.OK_OPTION
    }

    if (!DebuggerUI.isDebuggerStopped || userAcceptsDeadlockRisk) {
      if (action != "") {
        if (writeAction)
          write(action, inStyle)

        val ss = Option(DebuggerUI.replScenarioStack).getOrElse(EvaluationContext.scenarioStack)
        DebuggerUI.underStackOf(ss) {
          GraphConsole.interpret(interpreter, action) match {
            case Results.Success    => in.setText("")
            case Results.Error      =>
            case Results.Incomplete => in.setText("")
          }
        }
      }
      lastSetOfCandidates = Completions.NoCompletions
    }
  }

  private def updateTitle(): Unit = {
    if (DebuggerUI.replScenarioStack eq null) {
      title.setForeground(Color.red)
      title.setToolTipText(
        "<html>Currently only <b>minimal</b> ScenarioStack is provided.<br>" +
          "Right Click on a node and update ScenarioStack from the menu.")
    } else {
      title.setForeground(Color.black)
      title.setToolTipText("ScenarioStack is currently provided")
    }
  }

  private[optimus] def setDefaultScenarioStack(scenarioStack: ScenarioStack): Unit = {
    DebuggerUI.replScenarioStack = scenarioStack
    updateTitle()
    outBuffer.write("Default ScenarioStack updated in Console")
  }

  private def cmdExecuteSelection(): Unit = {
    val cmd = outpane.getSelectedText
    if (cmd != null && cmd.length() >= 0)
      execute(cmd, writeAction = false)
  }

  private def cmdSaveLog(): Unit = {
    JUIUtils.saveToFile(this, FileChooser.htmlLogFileChooser) { file =>
      val out = new FileWriter(file)
      outpane.write(out)
      out.close()
    }
  }

  private def fileOfLastLog = new File(System.getProperty("user.home"), "default.log.html")

  def windowClosing(): Unit = {
    if (GraphConsole.autoSave.get) {
      val fout = new FileWriter(fileOfLastLog)
      outpane.write(fout)
      fout.close()
    }
  }

  private def cmdLoadLog(): Unit = {
    JUIUtils.readFromFile(this, FileChooser.htmlLogFileChooser) { file =>
      loadLog(file)
    }
  }

  private def cmdLoadLastLog(): Unit = loadLog(fileOfLastLog)

  private def loadLog(file: File): Unit = {
    try {
      val fin = new FileReader(file)
      outpane.read(fin, "Log")
      resetBodyElm()
      fin.close()
    } catch {
      case ex: Exception =>
        JOptionPane.showMessageDialog(this, ex.toString, "Could not load log", JOptionPane.ERROR_MESSAGE);
    }
  }

  private def init(): Unit = {
    setBorder(new EmptyBorder(0, 5, 5, 5))
    setLayout(new BorderLayout(0, 1))

    title.setFont(new Font(title.getFont.getName, Font.BOLD, title.getFont.getSize))
    updateTitle()

    val toolBar = new JToolBar2()
    toolBar.add(title)
    toolBar.addSeparator(new Dimension(16, 0))
    toolBar.addSeparator()
    toolBar.addButton("Execute Selected", "Ctrl + Enter") { cmdExecuteSelection() }
    toolBar.addSeparator()
    toolBar.addButton("Save Log") { cmdSaveLog() }
    toolBar.addSeparator()
    toolBar.addButton("Load Log") { cmdLoadLog() }
    toolBar.addSeparator()
    toolBar.addButton("Load Last") { cmdLoadLastLog() }
    toolBar.addSeparator()
    toolBar.addCheckBox(GraphConsole.autoSave)
    toolBar.addSeparator()
    toolBar.addCheckBox(GraphConsole.truncate)
    toolBar.addSeparator()
    toolBar.addButton("Clear") { out.setOuterHTML(bodyElm, "<body></body"); resetBodyElm(); }
    toolBar.addSeparator()
    toolBar.add(createRecipesSelector())
    toolBar.addSeparator()

    add(toolBar, BorderLayout.NORTH)

    val panel = new JPanel()
    panel.setLayout(new BorderLayout(0, 0))

    in = new JTextField()
    val height = Math.round(12 * Fonts.multiplier) * 2
    in.setMaximumSize(new Dimension(2147483647, height))
    in.setPreferredSize(new Dimension(300, height))
    in.setFocusTraversalKeysEnabled(false)
    in.addKeyListener(new KeyAdapter() {
      override def keyPressed(e: KeyEvent): Unit = {
        val keyCode = e.getKeyCode
        if (keyCode == KeyEvent.VK_TAB) autoComplete(false)
      }

      override def keyTyped(e: KeyEvent): Unit = {
        if (lastSetOfCandidates ne Completions.NoCompletions) {
          val keyChar = e.getKeyChar
          if (e.getKeyCode == KeyEvent.VK_TAB) {}
          if (keyChar.isControl) {} else if (keyChar.isLetterOrDigit) {
            val len = in.getSelectionStart - lastSetOfCandidates.cursor
            if (len > 0) {
              val prefix = in.getText(lastSetOfCandidates.cursor, len)
              val suggest = prefix + keyChar
              val x = lastSetOfCandidates.candidates.find(c => c.startsWith(suggest))
              if (x.isDefined) {
                in.replaceSelection(x.get.substring(len))
                in.select(lastSetOfCandidates.cursor + len + 1, lastSetOfCandidates.cursor + x.get.length())
                e.consume()
              }
            }
          } else
            in.setSelectionStart(in.getSelectionEnd)
        }
      }

      override def keyReleased(e: KeyEvent): Unit = {
        if (e.getKeyCode == KeyEvent.VK_PERIOD) autoComplete(true)
      }

    })

    in.addActionListener((ae: ActionEvent) => {
      if (DiagnosticSettings.outOfProcess)
        GraphDebuggerUI.showMessage("REPL doesn't support out of process yet")
      else
        execute(ae.getActionCommand)
    })
    panel.add(in, BorderLayout.CENTER)

    val scrollPane = new JScrollPane()
    add(panel, BorderLayout.SOUTH)
    add(scrollPane, BorderLayout.CENTER)

    outpane = new JTextPane()
    outpane.setContentType("text/html")
    outpane.setAutoscrolls(true)
    outpane.setText(
      "<html><head>" + GraphConsole.startStyle + "</head><body>" + GraphConsole.startBanner + "</body</html>")
    outpane.addKeyListener(new KeyAdapter() {
      override def keyPressed(e: KeyEvent): Unit = {
        if (e.getKeyCode == KeyEvent.VK_ENTER && e.isControlDown)
          cmdExecuteSelection()
      }
    })
    resetBodyElm()
    scrollPane.setViewportView(outpane)

    out.getElement("1")
  }

  def createRecipesSelector(): JButton = {
    val menu = new JPopupMenu2
    for (r <- Recipes.recipes) {
      menu.addMenu(r.title, r.comment) { write(r.code, inStyle) }
    }

    class XToggleButton extends JButton("Recipes") {
      override def processMouseEvent(e: MouseEvent): Unit = {
        if (e.getID == MouseEvent.MOUSE_PRESSED && e.getButton == MouseEvent.BUTTON1) {
          menu.show(this, 0, this.getHeight)
        } else {
          super.processMouseEvent(e)
        }
      }
    }

    val button = new XToggleButton()
    button.setToolTipText("Common helper algorithms to analyse profile information")
    button
  }

  private[ui] def setResultLambda(tpe: Class[_ <: AnyRef], expr: String): Unit =
    execute(GraphConsole.prepareResultLambda(tpe, expr))
}
