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
package optimus.debugger.browser.ui

import java.util.UUID
import javax.swing.JTextPane
import optimus.core.CoreHelpers
import optimus.core.CoreHelpers.safeToString
import optimus.graph.PropertyNode
import optimus.graph.RecordedTweakables
import optimus.platform.util.html._
import optimus.profiler.DebuggerUI
import GraphBrowserAPI._

import javax.swing.JDialog
import javax.swing.JLabel
import javax.swing.JTextArea
import javax.swing.WindowConstants
import optimus.graph.diagnostics.GraphDebuggerValueDetailsViewable

import javax.swing.UIManager
import optimus.graph.TweakTreeNode
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.ScenarioStack
import optimus.platform.Tweak
import optimus.platform.storable.EntityPrettyPrintView
import optimus.profiler.ui.SClipboard
import optimus.profiler.extensions.PNodeTaskExtension._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object DebuggerViewModel {
  private[optimus] def customizeViewHyperLink(
      ss: ScenarioStack,
      viewable: GraphDebuggerValueDetailsViewable,
      title: String): Link = {
    Link(
      UUID.randomUUID().toString,
      "more details",
      Some(() =>
        DebuggerUI.underStackOfWithoutNodeTracing(ss) {
          try {
            val jd: JDialog = new JDialog()
            jd.setSize(1500, 900)
            jd.setTitle(title)
            jd.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)
            jd.add(viewable.view(title))
            jd.setVisible(true)
          } catch {
            case NonFatal(e) =>
              val errorDialog = new JDialog()
              errorDialog.setTitle("Error")
              errorDialog.setSize(200, 100)
              errorDialog.add(new JLabel("Cannot display this node"))
              val errorMessage = new JTextArea()
              errorMessage.setText(e.toString)
              errorMessage.setWrapStyleWord(true)
              errorMessage.setLineWrap(true)
              errorMessage.setOpaque(false)
              errorMessage.setEditable(false)
              errorMessage.setBackground(UIManager.getColor("Label.background"))
              errorMessage.setFont(UIManager.getFont("Label.font"))
              errorMessage.setBorder(UIManager.getBorder("Label.border"))
              errorDialog.add(errorMessage)
              errorDialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)
              errorDialog.setVisible(true)
          }
        })
    )
  }

}

private[debugger] class DebuggerViewModel(val interpreter: HtmlInterpreters.Type, val review: NodeReview) {
  private def writeTweakDiffs(hb: HtmlBuilder): Unit = if (review.extraTweaks ne null) {
    if (review.extraTweaks.nonEmpty || review.missingTweaks.nonEmpty || review.mismatchedTweaks.nonEmpty) {
      hb.namedGroup("TweakDiffs", separateBy = 2) {
        if (review.extraTweaks.nonEmpty) {
          hb.block {
            hb.add(TweakHeader("Extra Tweaks")).newLine()
            writeTweakTreeNodes(hb, review.extraTweaks)
          }
        }

        if (review.missingTweaks.nonEmpty) {
          hb.block {
            hb.add(TweakHeader("Missing Tweaks")).newLine()
            writeTweakTreeNodes(hb, review.missingTweaks)
          }
        }

        if (review.mismatchedTweaks.nonEmpty) {
          hb.block {
            hb.add(TweakHeader("Mismatched Tweaks")).newLine()
            writeTweaks(hb, review.mismatchedTweaks)
          }
        }
      }
    }
  }

  // write a tweak in the form: MyEntity(@hashcode).node :=someValue (note that for by name lambdas someValue
  // will be printed as reference to the line of code where the tweak is defined)
  private def writeTweak(hb: HtmlBuilder, twk: PropertyNode[_], tweakTo: Option[Tweak] = None): Unit = {
    hb ++= twk.entity.getClass.getSimpleName ++= "("
    hb ++= TweakLow("@" + twk.entity.hashCode().toHexString)
    hb += ")." += twk.propertyInfo.name() += " "

    if (tweakTo.nonEmpty) {
      hb ++= TweakVal(tweakTo.get.tweakTemplate.toString)
    } else if (twk.isDoneWithResult) {
      hb ++= ":=" ++= TweakVal(CoreHelpers.safeToString(twk.resultObject()))
    }
  }

  // print missing or extra tweaks
  private def writeTweakTreeNodes(hb: HtmlBuilder, col: Iterable[TweakTreeNode]): Unit = {
    val it = col.iterator
    while (it.hasNext) {
      val ttn = it.next()
      writeTweak(hb, ttn.key, Some(ttn.tweak))
      hb.newLine()
    }
  }

  // print mismatched tweaks
  private def writeTweaks(hb: HtmlBuilder, col: ArrayBuffer[(TweakTreeNode, TweakTreeNode)]): Unit = {
    var i = 0
    while (i < col.length) {
      val (first, second) = col(i)
      writeTweak(hb, first.key, Some(first.tweak))
      hb ++= " did not match "
      writeTweak(hb, second.key, Some(second.tweak))
      i += 1
    }
  }

  private def writeXSTweaks(hb: HtmlBuilder): Unit = {
    review.task.scenarioStack.tweakableListener match {
      case rs: RecordedTweakables => rs.writeHtml(hb)
      case _                      =>
    }
  }

  private def entityTemporalInfo(hb: HtmlBuilder, outpane: JTextPane): Unit = {
    val resultEntity = review.task.resultEntity
    if ((resultEntity ne null) && !resultEntity.dal$isTemporary) {
      hb.namedGroup("TemporalInfo", separateBy = 2) {
        if (resultEntity.isTxValidTime) {
          hb ++= TimeHeader("ValidTime = TransactionTime")
          hb ++= TimeValue(resultEntity.txTime.toString)
        } else {
          hb ++= TimeHeader("ValidTime")
          hb ++= TimeValue(resultEntity.validTime.toString)
          hb.newLine()
          hb ++= TimeHeader("TransactionTime")
          hb ++= TimeValue(resultEntity.txTime.toString)
        }
      }

      // display custom view
      if (customView(resultEntity) ne null) {
        hb.forceNewLine()
        outpane.insertComponent(customView(resultEntity))
        hb.forceNewLine()
      }
    }
  }

  private def nodeArguments(hb: HtmlBuilder, wrap: Boolean): Unit =
    if (review.args.nonEmpty) {
      hb.namedGroup("Arguments", separateBy = 2) {
        var i = 0
        while (i < review.args.length) {
          val a = review.args(i)
          hb ++= ArgumentHeader("arg " + i)
          hb.newLine()
          val argStr = safeToString(a)
          if (wrap)
            hb ++= ("ArgumentValue", argStr)
          else
            hb ++= PreFormatted(argStr)

          a match {
            case Some(viewable: GraphDebuggerValueDetailsViewable) =>
              hb.add(
                DebuggerViewModel.customizeViewHyperLink(
                  review.task.scenarioStack,
                  viewable,
                  "" + review.task.nodeName() + " argument: " + viewable.toString))
            case viewable: GraphDebuggerValueDetailsViewable =>
              hb.add(
                DebuggerViewModel.customizeViewHyperLink(
                  review.task.scenarioStack,
                  viewable,
                  "" + review.task.nodeName() + " argument: " + viewable.toString))
            case _ =>
          }
          hb.newLine()
          i += 1
        }
      }
    }

  private def nodeEntity(hb: HtmlBuilder, showProps: Boolean): Unit = {
    val entity = review.task.getEntity
    if (entity ne null) {
      hb.namedGroup("Entity", separateBy = 2) {
        hb ++= EntityHeader("Entity")
        hb ++= EntityClassName(entity.getClass.getName)
        if (showProps)
          new EntityPrettyPrintView(entity).dumpProps(hb)
        entity match {
          case detailsViewable: GraphDebuggerValueDetailsViewable =>
            hb ++= DebuggerViewModel.customizeViewHyperLink(review.task.scenarioStack, detailsViewable, entity.toString)
          case _ =>
        }
      }
    }
  }

  private def scenarioStack(ss: ScenarioStack, hb: HtmlBuilder, minDepth: Int): Unit = {
    if (ss ne null)
      ss.writeHtml(hb, minDepth)
    else
      hb ++= "empty"
  }

  private def scenarioStacks(hb: HtmlBuilder, minDepth: Int): Unit =
    hb.namedGroup("ScenarioStacks", separateBy = 2) {
      val task = review.task
      val handler = () => {
        val html = new HtmlBuilder(hb.cfg)
        scenarioStack(task.scenarioStack, html, minDepth)
        SClipboard.copy(html.toString, html.toPlaintext)
        ()
      }
      val copyLink = Link(contents = "[copy]", href = s"copy_scenario_stack${review.task.id}", handler = Some(handler))
      hb.add(SStackHeader("Scenario Stacks")).add(copyLink).newLine()
      scenarioStack(task.scenarioStack, hb, minDepth)
    }

  def nodeDetails(hb: HtmlBuilder, showIndex: Boolean, minScenarioStackDepth: Int, outpane: JTextPane): HtmlBuilder = {
    if (showIndex) hb ++= Break("#" + review.task.id)
    if (hb.cfg.tweakDiffs) writeTweakDiffs(hb)
    if (hb.cfg.xsTweaks) writeXSTweaks(hb)

    if (hb.cfg.result) {
      hb.separated(2) {
        if (hb.cfg.wrap)
          hb.noStyle("NodeResult", review.task.resultDisplayString)
        else
          hb ++= PreFormatted(review.task.resultDisplayString)

        review.task.resultKey() match {
          case Some(viewable: GraphDebuggerValueDetailsViewable) =>
            hb.add(DebuggerViewModel.customizeViewHyperLink(review.task.scenarioStack, viewable, viewable.toString))
          case viewable: GraphDebuggerValueDetailsViewable =>
            hb.add(DebuggerViewModel.customizeViewHyperLink(review.task.scenarioStack, viewable, viewable.toString))
          case _ =>
        }
      }

      hb.newLine()
    }

    if (hb.cfg.resultEntityTemporalInfo) entityTemporalInfo(hb, outpane)
    if (hb.cfg.args) nodeArguments(hb, hb.cfg.wrap)
    if (hb.cfg.entity) nodeEntity(hb, hb.cfg.properties)
    if (hb.cfg.scenarioStack) scenarioStacks(hb, minScenarioStackDepth)

    hb
  }
}

object DebuggerViewModelHtml {
  def apply(node: PNodeTask): DebuggerViewModel = {
    new DebuggerViewModel(HtmlInterpreters.prod, new NodeReview(node))
  }

  def apply(nodeReview: NodeReview): DebuggerViewModel = new DebuggerViewModel(HtmlInterpreters.prod, nodeReview)
}
