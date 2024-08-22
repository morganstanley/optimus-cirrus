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

import optimus.graph.diagnostics.DbgPreference
import optimus.graph.diagnostics.Debugger
import optimus.profiler.ui.Fonts
import optimus.profiler.ui.common.JTabbedPane2.darkBlue
import optimus.profiler.ui.common.JTabbedPane2.propertyChangeEvent

import java.awt.BorderLayout
import java.awt.Color
import java.awt.Component
import java.awt.Dimension
import java.awt.FlowLayout
import java.awt.Font
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.Insets
import java.awt.LayoutManager
import java.awt.Rectangle
import java.awt.RenderingHints
import java.awt.event.ActionEvent
import java.awt.event.ComponentEvent
import java.awt.event.ComponentListener
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import java.beans.PropertyChangeEvent
import java.util
import java.util.prefs.Preferences
import javax.swing.JButton
import javax.swing.JCheckBox
import javax.swing.JCheckBoxMenuItem
import javax.swing.JComponent
import javax.swing.JFrame
import javax.swing.JMenu
import javax.swing.JMenuItem
import javax.swing.JOptionPane
import javax.swing.JPanel
import javax.swing.JPopupMenu
import javax.swing.JSplitPane
import javax.swing.JTabbedPane
import javax.swing.JToolBar
import javax.swing.MenuElement
import javax.swing.MenuSelectionManager
import javax.swing.RowSorter
import javax.swing.RowSorter.SortKey
import javax.swing.SortOrder
import javax.swing.event.ChangeEvent
import javax.swing.event.MenuEvent
import javax.swing.event.MenuListener
import javax.swing.event.PopupMenuEvent
import javax.swing.event.PopupMenuListener
import javax.swing.table.TableModel
import javax.swing.table.TableRowSorter
import scala.collection.mutable

trait JMenuOps {
  private val items = mutable.Map[String, JMenuItem]()

  def add(item: JMenuItem): JMenuItem
  def addOnPopup(updateMenu: => Unit): Unit = {}

  private def addInternal(item: JMenuItem): JMenuItem = {
    items.put(item.getText, item)
    add(item)
  }

  /** Add an 'advanced' menu, that is enabled by checking off 'Show Advanced Menu' */
  final def addAdvMenu(title: String)(action: => Unit): JMenuItem = {
    val mi = addMenu(title, null, action, enable = true)
    addOnPopup(mi.setVisible(Debugger.dbgShowAdvancedCmds.get))
    mi
  }

  final def addAdvCheckBoxMenu(title: String, toolTip: String, dbgPref: DbgPreference)(
      action: Boolean => Unit): JMenuItem = {
    val mi = addCheckBoxMenu(title, toolTip, dbgPref, action)
    addOnPopup(mi.setVisible(Debugger.dbgShowAdvancedCmds.get))
    mi
  }

  final def addMenu(title: String)(action: => Unit): JMenuItem =
    addMenu(title, null, action, enable = true)

  final def addMenu(title: String, action: => Unit, enable: Boolean = true): JMenuItem =
    addMenu(title, null, action, enable)

  final def addMenu(title: String, toolTip: String)(action: => Unit): JMenuItem =
    addMenu(title, toolTip, action, enable = true)

  def addMenu(title: String, toolTip: String, action: => Unit, enable: Boolean): JMenuItem = {
    val mi = new JMenuItem(title)
    mi.addActionListener((_: ActionEvent) => { action })
    mi.setToolTipText(toolTip)
    mi.setEnabled(enable)
    addInternal(mi)
    mi
  }

  def addStayOpenCheckBoxMenu(title: String, toolTip: String, checked: Boolean)(
      action: Boolean => Unit): StayOpenCheckBoxMenuItem = {
    val mi = new StayOpenCheckBoxMenuItem(title)
    mi.addActionListener(_ => action(mi.isSelected))
    mi.setToolTipText(toolTip)
    if (checked) mi.setSelected(checked)
    addInternal(mi)
    mi
  }

  def addCheckBoxMenu(title: String, toolTip: String, checked: Boolean)(action: => Unit): JCheckBoxMenuItem = {
    val mi = new JCheckBoxMenuItem(title)
    mi.addActionListener(_ => action)
    mi.setToolTipText(toolTip)
    if (checked) mi.setSelected(checked)
    addInternal(mi)
    mi
  }

  def addCheckBoxMenu(pref: DbgPreference): JCheckBoxMenuItem =
    addCheckBoxMenu(pref.title, pref.tooltip, pref, _ => ())

  def addCheckBoxMenu(title: String, toolTip: String, pref: DbgPreference): JCheckBoxMenuItem =
    addCheckBoxMenu(title, toolTip, pref, _ => ())

  def addCheckBoxMenu(title: String, toolTip: String, pref: DbgPreference, f: Boolean => Unit): JCheckBoxMenuItem = {
    val mi = new JCheckBoxMenuItem2(title, toolTip, pref, f)
    addOnPopup(mi.setSelected(pref.get))
    addInternal(mi)
    mi
  }

  def getMenu(title: String): Option[JMenuItem] = items.get(title)
}

class JCheckBoxMenuItem2(title: String, toolTip: String, pref: DbgPreference, action: Boolean => Unit)
    extends JCheckBoxMenuItem(title) {

  addActionListener(_ => { pref.set(isSelected); action(pref.get) })
  setToolTipText(toolTip)
  if (pref.get) setSelected(true)
  action(isSelected)
}

class JMenu2(title: String) extends JMenu(title) with JMenuOps {
  override def addOnPopup(updateMenu: => Unit): Unit = {
    addMenuListener(new MenuListener {
      override def menuSelected(e: MenuEvent): Unit = { updateMenu }
      override def menuDeselected(e: MenuEvent): Unit = {}
      override def menuCanceled(e: MenuEvent): Unit = {}
    })
  }
}

class JPopupMenu2 extends JPopupMenu with JMenuOps {

  def addCheckBoxMenu(title: String, stateUpdate: => Boolean, action: JCheckBoxMenuItem => Unit): JCheckBoxMenuItem = {
    val mi = new JCheckBoxMenuItem(title)
    addPopupMenuListener(new PopupMenuListener {
      def popupMenuWillBecomeVisible(e: PopupMenuEvent): Unit = { mi.setState(stateUpdate) }
      def popupMenuWillBecomeInvisible(e: PopupMenuEvent): Unit = {}
      def popupMenuCanceled(e: PopupMenuEvent): Unit = {}
    })
    mi.addActionListener((_: ActionEvent) => { action(mi) })
    add(mi)
    mi
  }

  override def addOnPopup(updateMenu: => Unit): Unit = {
    addPopupMenuListener(new PopupMenuListener {
      override def popupMenuWillBecomeVisible(e: PopupMenuEvent): Unit = { updateMenu }
      override def popupMenuWillBecomeInvisible(e: PopupMenuEvent): Unit = {}
      override def popupMenuCanceled(e: PopupMenuEvent): Unit = {}
    })
  }
}

class JToolBar2(caption: String) extends JToolBar(caption) {
  setFloatable(false)
  setMinimumSize(new Dimension(1, 1)) // Make sure we can always resize down and ignore preferred size

  def this() = this(null)

  def addButton(name: String, toolTip: String = null, enabled: Boolean = true)(action: => Unit): JButton = {
    addButtonEx(name, toolTip, enabled) { _: ActionEvent =>
      action
    }
  }

  def addButtonEx(name: String, toolTip: String, enabled: Boolean)(action: ActionEvent => Unit): JButton = {
    val btn = new JButton(name)
    if (toolTip ne null) btn.setToolTipText(toolTip)
    btn.setEnabled(enabled)
    btn.addActionListener((e: ActionEvent) => action(e))
    add(btn)
    btn
  }

  def addCheckBox(name: String, checked: Boolean, action: Boolean => Unit): JCheckBox = {
    val btn = new JCheckBox(name, checked)
    btn.addActionListener((_: ActionEvent) => action(btn.isSelected))
    add(btn)
    btn
  }

  def addCheckBox(pref: DbgPreference): JCheckBox = {
    val btn = new JCheckBox(pref.name, pref.get)
    btn.addActionListener((_: ActionEvent) => pref.set(btn.isSelected))
    add(btn)
    btn
  }
}

class JSplitPane2(pref: Preferences, name: String = "split", defDivLocation: Int = 400) extends JSplitPane {
  setBorder(null)
  setDividerSize(3)
  setDividerLocation(pref.getInt(name, defDivLocation))

  override def setDividerLocation(newLocation: Int): Unit = {
    super.setDividerLocation(newLocation)
    pref.putInt(name, newLocation)
  }
}

class JPanel2(layout: LayoutManager) extends JPanel(layout) with ConfigSettings {

  setFont(getFont.deriveFont(Fonts.multiplier))

  def this() = this(new FlowLayout())

  def addButton(name: String)(action: => Unit): JButton = {
    val btn = new JButton(name)
    btn.addActionListener((_: ActionEvent) => action)
    add(btn)
    btn
  }
}

object JTabbedPane2 {
  // we create variable for our fireproperty
  val propertyChangeEvent: String = "repaintSelectedTabWithBlueHighlight"
  val darkBlue: Color = new Color(63, 72, 204)
  val closeableTabLayout = new FlowLayout(FlowLayout.CENTER, 0, 0)
  val closeBtnInsets = new Insets(-2, -2, -2, -2)
  val closeBtnSize = new Dimension(15, 15)
  val closeBtnFont = new Font(null, Font.PLAIN, 10)
}

final class JTabbedPane2(preferences: Preferences) extends JTabbedPane {
  private def highLightSelectedTab(index: Int, oldIndex: Int): Unit = {
    setColor(oldIndex, Color.BLACK)
    setColor(index, darkBlue)
  }

  private def setColor(index: Int, color: Color): Unit = {
    if (index != -1) {
      val component = getComponentAt(index)
      setForegroundAt(index, color)
      // we can't capture the tab normally because it's custom, we add a fireProperty and add this propertyChangeEvent to the tab.
      // the change event won't be fired if the new value is the same as the old value so these values must be different
      component.firePropertyChange(propertyChangeEvent, 1, 2)
    }
  }

  def addCloseableTab(title: String, component: JComponent, onCloseTab: () => Unit): Component = {
    val tab = add(title, component)
    val index = indexOfComponent(tab)

    def closeTab(): Unit = {
      if (onCloseTab ne null) onCloseTab()
      removeTabAt(indexOfComponent(tab))
    }

    val closeBtn = new JButton("\u2716") // ?
    closeBtn.setMargin(JTabbedPane2.closeBtnInsets)
    closeBtn.setPreferredSize(JTabbedPane2.closeBtnSize)
    closeBtn.setFont(JTabbedPane2.closeBtnFont)
    closeBtn.setContentAreaFilled(false)
    closeBtn.setForeground(Color.GRAY)
    closeBtn.addActionListener((_: ActionEvent) => closeTab())
    closeBtn.addMouseListener(new MouseAdapter {
      override def mouseExited(arg0: MouseEvent): Unit = closeBtn.setForeground(Color.GRAY)
      override def mouseEntered(arg0: MouseEvent): Unit = closeBtn.setForeground(Color.DARK_GRAY)
    })

    val closeableTab = new JPanel(JTabbedPane2.closeableTabLayout)
    closeableTab.setOpaque(false)
    val label = new JButton(title)
    label.setFocusPainted(false) // these changes make it look like JLabel
    label.setMargin(new Insets(0, 0, 0, 0))
    label.setContentAreaFilled(false)
    label.setBorderPainted(false)
    label.setOpaque(false)
    label.addActionListener { (_: ActionEvent) => setSelectedComponent(indexOfComponent(tab), label) }
    label.setComponentPopupMenu(initPopupMenu(tab, label))
    label.addMouseListener(new MouseAdapter {
      override def mouseClicked(e: MouseEvent): Unit = {
        if (e.getButton == MouseEvent.BUTTON2) { closeTab(); e.consume() } // middle-click to close
      }
    })
    // we add a propertyChange with the variable we created and when it's not selected we make the foreground change
    tab.addPropertyChangeListener(propertyChangeEvent, (_: PropertyChangeEvent) => label.setForeground(Color.BLACK))

    // switch to newly created tab and change color
    setSelectedComponent(index, label)

    closeableTab.add(label, BorderLayout.WEST)
    closeableTab.add(closeBtn, BorderLayout.EAST)

    setTabComponentAt(index, closeableTab)
    tab
  }

  private def initPopupMenu(tab: Component, titleLabel: JButton): JPopupMenu2 = {
    val menu = new JPopupMenu2
    menu.addMenu(
      "Set Title", {
        JOptionPane.showInputDialog(this, "Enter New Title", titleLabel.getText) match {
          case s: String => titleLabel.setText(s)
          case _         =>
        }
      })
    menu.addMenu(
      "Detach", {
        val sz = tab.getSize
        remove(tab)
        val frame = new JFrame
        frame.setContentPane(tab.asInstanceOf[JPanel]) // our tabs are all JPanel2's
        frame.setTitle(titleLabel.getText)
        frame.setSize(sz)
        frame.setVisible(true)
      }
    )
    menu.addMenu("Close", remove(tab))
    menu
  }

  // we add another parameter so we can access label which is what we're trying to access and add the color
  private def setSelectedComponent(index: Int, component: Component): Unit = {
    setSelectedIndex(index)
    if (component != null) // created a closeable tab with a label, and we need to set its color, not the tab's
      component.setForeground(darkBlue)
  }

  override def setSelectedIndex(index: Int): Unit = {
    val currentTab = getSelectedIndex
    highLightSelectedTab(index, currentTab)
    if (currentTab != -1)
      preferences.putInt("activeTab", index)
    super.setSelectedIndex(index)
  }

  def restoreSelectedIndex(): Unit = {
    val activeTab = preferences.getInt("activeTab", 0)
    if (activeTab < getTabCount)
      setSelectedIndex(activeTab)
  }

  /** add new tab with given title at the selected index */
  def add(title: String, component: Component, index: Int): Unit =
    insertTab(title, null, component, null, index)
}

trait ProvidesMenus {
  def add(menu: JMenu, title: String, action: => Unit): JMenuItem = {
    val mi = menu.add(title)
    mi.addActionListener((_: ActionEvent) => action)
    mi
  }

  def add(menu: JMenu, title: String, action: => Unit, tooltipText: Option[String] = None): JMenuItem = {
    val mi = menu.add(title)
    mi.addActionListener(_ => action)
    tooltipText foreach { mi.setToolTipText }
    mi
  }

  def getMenus: Seq[JMenu] = Nil
}

private[ui] class JFrame2 extends JFrame with ConfigSettings {
  import java.awt.GraphicsEnvironment
  {
    var x = pref.getInt("x", 0)
    var y = pref.getInt("y", 0)
    val screens = GraphicsEnvironment.getLocalGraphicsEnvironment.getScreenDevices
    var i = 0
    while (i < screens.length) {
      if (screens(i).getDefaultConfiguration.getBounds.contains(x, y)) i = Integer.MAX_VALUE // break
      else i += 1
    }
    if (i != Integer.MAX_VALUE) {
      x = 0
      y = 0
    }
    setBounds(x, y, pref.getInt("width", 1550), pref.getInt("height", 600))
    val exState = pref.getInt("exState", 0)
    if (exState != 0) setExtendedState(exState)
  }

  addComponentListener(new ComponentListener {
    private def putExStateInPref(): Int = {
      val exState = getExtendedState
      pref.putInt("exState", exState)
      exState
    }

    def componentMoved(evt: ComponentEvent): Unit = {
      val b = JFrame2.this.getBounds()
      val exState = putExStateInPref()
      if (exState == 0) {
        pref.putInt("x", b.x)
        pref.putInt("y", b.y)
      }
    }
    def componentShown(evt: ComponentEvent): Unit = {}
    def componentResized(evt: ComponentEvent): Unit = {
      val b = JFrame2.this.getBounds()
      val exState = putExStateInPref()
      if (exState == 0) {
        pref.putInt("width", b.width)
        pref.putInt("height", b.height)
      }
    }
    def componentHidden(evt: ComponentEvent): Unit = {}
  })

}

trait HasMessageBanners extends JComponent {
  private[this] var msgLines: Seq[String] = _
  private[this] var msgAlpha = 240
  private[this] var msgRect = new Rectangle(0, 0, 0, 0)
  private[this] val msgFont = new Font("Arial", Font.PLAIN, 24)
  private[this] var msgTimer: javax.swing.Timer = _
  private[this] val msgBorder = 20

  def messageChanged(newMsg: String): Unit = {}

  private def setMsgLines(msg: String): Unit = {
    msgLines = if (msg ne null) msg.split("\n") else null
  }

  def setMessage(msg: String): Unit = {
    if (msg == null && msgLines == null) return
    msgRect = paintRectangle(getGraphics, msg)
    setMsgLines(msg)
    messageChanged(msg)
    msgAlpha = 240
    HasMessageBanners.this.repaint(msgRect)
  }

  def showMessage(msg: String): Unit = {
    msgRect = paintRectangle(getGraphics, msg)
    setMsgLines(msg)
    messageChanged(msg)
    msgAlpha = 240
    HasMessageBanners.this.paintImmediately(msgRect)
    if (msgTimer eq null)
      hideMessage()
    msgTimer.start()
  }

  def hideMessage(): Unit = {
    msgTimer = new javax.swing.Timer(
      10,
      (_: ActionEvent) => {
        val invalidateRect = new Rectangle(msgRect)
        invalidateRect.grow(2, 2)
        HasMessageBanners.this.repaint(invalidateRect)
        msgAlpha -= 5
        if (msgAlpha < 20) {
          setMsgLines(null)
          messageChanged(null)
          msgTimer.stop()
        }
      }
    )
    msgTimer.setInitialDelay(3000)
    msgTimer.setRepeats(true)
  }

  private def multiLineString(g: Graphics2D): Unit = {
    val x = msgRect.x + msgBorder
    var y = msgRect.y + msgRect.height - msgBorder - 5 // Note: ad-hoc baseline
    if (msgLines ne null)
      msgLines.foreach { line =>
        g.drawString(line, x, y)
        y += g.getFontMetrics.getHeight // increment after because top line coord is OK
      }
  }

  // note - use tail because we don't want to count first line as 'extra' height
  private def extraHeight(g: Graphics2D, str: String): Int =
    if (str ne null) str.split("\n").tail.length * g.getFontMetrics(msgFont).getHeight
    else 0

  private def extraHeight(g: Graphics2D): Int =
    if (msgLines ne null) (msgLines.length - 1) * g.getFontMetrics(msgFont).getHeight
    else 0

  override protected def paintComponent(g1: Graphics): Unit = {
    super.paintComponent(g1)
    if (msgLines ne null) {
      msgRect = paintRectangle(g1, null)
      val g = g1.asInstanceOf[Graphics2D]
      g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
      g.setColor(new Color(64, 64, 64, msgAlpha))
      g.fillRoundRect(msgRect.x, msgRect.y + extraHeight(g), msgRect.width, msgRect.height, 15, 15)
      g.setFont(msgFont)
      g.setColor(Color.WHITE)
      multiLineString(g)
    }
  }

  private def paintRectangle(g: Graphics, str: String) = {
    if (g eq null) new Rectangle(0, 0, 0, 0)
    else {
      val g2 = g.asInstanceOf[Graphics2D]
      val empty = new Rectangle()
      val sb1 =
        if (msgLines eq null) empty
        else msgFont.getStringBounds(msgLines.maxBy(_.length), g2.getFontRenderContext).getBounds
      val sb2 = if (str eq null) empty else msgFont.getStringBounds(str, g2.getFontRenderContext).getBounds
      sb1.height += extraHeight(g2)
      sb2.height += extraHeight(g2, str)
      val sb = new Rectangle(0, 0, Math.max(sb1.width, sb2.width), Math.max(sb1.height, sb2.height))
      val vrect = getVisibleRect

      val x = vrect.x + (vrect.width - sb.width - msgBorder * 2) / 2
      val y = vrect.y + (vrect.height - sb.height - msgBorder * 2) / 2
      val width = sb.width + msgBorder * 2
      val height = sb.height + msgBorder * 2

      new Rectangle(x, y, width, height)
    }
  }
}

class StayOpenCheckBoxMenuItem(text: String) extends JCheckBoxMenuItem(text) {
  private var path: Array[MenuElement] = _
  getModel.addChangeListener((_: ChangeEvent) => {
    if (getModel.isArmed && isShowing) path = MenuSelectionManager.defaultManager().getSelectedPath
  })

  override def doClick(pressTime: Int): Unit = {
    super.doClick(pressTime)
    MenuSelectionManager.defaultManager.setSelectedPath(path)
  }
}

// https://stackoverflow.com/questions/37000749
final class DescendingOrderSorter(model: TableModel, copyTo: RowSorter[_]) extends TableRowSorter(model) {
  override def toggleSortOrder(column: Int): Unit = {
    val sortKeys = new util.ArrayList[SortKey](getSortKeys)
    if (sortKeys.isEmpty || sortKeys.get(0).getColumn != column) {
      sortKeys.add(0, new RowSorter.SortKey(column, SortOrder.DESCENDING))
    } else if (sortKeys.get(0).getSortOrder == SortOrder.ASCENDING) {
      sortKeys.removeIf(key => key.getColumn == column)
      sortKeys.add(0, new SortKey(column, SortOrder.DESCENDING))
    } else {
      sortKeys.removeIf(key => key.getColumn == column)
      sortKeys.add(0, new SortKey(column, SortOrder.ASCENDING))
    }
    setSortKeys(sortKeys)
    copyTo.setSortKeys(sortKeys)
  }
}
