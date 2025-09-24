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
package optimus.platform.storable

import optimus.platform.EvaluationContext
import optimus.entity.StorableInfo
import optimus.platform.util.html.HtmlBuilder
import optimus.platform.util.html.NoStyleNamed

import scala.util.Try

class EntityPrettyPrintView(val e: Entity) extends AnyVal {
  private def headerVersion: String = e.dal$storageInfo.descriptorString

  def prettyPrint: String = "Entity " + e.getClass.getName + headerVersion + "\n" + dumpProps

  def dumpProps: String = dumpProps(new HtmlBuilder).toPlaintext
  def propsSeq: Seq[String] = dumpProps(new HtmlBuilder).toPlaintextSeq

  def dumpProps(hb: HtmlBuilder): HtmlBuilder = {
    import optimus.graph._

    hb.namedGroup("EntityProperties", separateBy = 1) {
      val info = e.$info
      hb ++= ("TemporalContext", {
        if (!e.dal$isTemporary)
          e.dal$temporalContext.toString
        else
          e.dal$storageInfo.descriptorString
      })
      hb.newLine()

      val keyPropNames = info match {
        case si: StorableInfo => Try(si.keys.flatMap(k => k.propertyNames)).getOrElse(Nil)
        case _                => Nil
      }
      def isKeyProp(p: NodeTaskInfo) = keyPropNames.contains(p.name)

      def addNode(name: String, nodeKey: NodeKey[_]) = {
        val (nodeVal, tweaked) =
          try {
            val node = EvaluationContext.lookupNode(nodeKey)

            if (DiagnosticSettings.evaluateNodeOnTouch) {
              (node.get, node.isInstanceOf[TweakNode[_]])
            } else {
              (node.resultAsStringOrElse("Not evaluated"), node.isInstanceOf[TweakNode[_]])
            }
          } catch {
            case _: IllegalScenarioDependenceException => ("Not available in SI context", false)
            case e: Throwable                          => (e, false)
          }
        addValue(name, nodeVal, tweaked)
      }

      def addValue(name: String, nodeVal: Any, tweaked: Boolean) = {
        hb.buildLeaf(NoStyleNamed.unseparated("EntityProperty")) { sb =>
          sb ++= name
          if (tweaked) sb ++= "(Tweaked)"
          if (keyPropNames contains name) sb ++= "(Key)"
          sb ++= ": " + nodeVal
        }
        hb.newLine()
      }
      Try(info.properties).getOrElse(Nil).sortBy(pi => if (isKeyProp(pi)) 0 else 1) foreach {
        case dpi: PropertyInfo0[_, _] =>
          addNode(dpi.name, dpi.asInstanceOf[PropertyInfo0[Entity, _]].createNodeKey(e))
        case nt: ReallyNontweakablePropertyInfo[_, _] =>
          addValue(nt.name, nt.asInstanceOf[ReallyNontweakablePropertyInfo[Entity, _]].getValue(e), tweaked = false)
        case _ =>
      }
    }
    hb
  }
}
