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
package optimus.tools.scalacplugins.entity

import optimus.platform.internal.annotations.forwarder

import scala.tools.nsc._
import plugins._
import transform._

/** Implements the behavior of @[[forwarder]]. See doc comment there. */
class ForwardingComponent(
    val pluginData: PluginData,
    val global: Global,
    val phaseInfo: OptimusPhaseInfo,
) extends PluginComponent
    with WithOptimusPhase
    with Transform
    with TypingTransformers {
  import global._

  def newTransformer(unit: CompilationUnit): Transformer = new TypingTransformer(unit) {
    override def transform(tree: Tree): Tree = tree match {
      case Forwarded(ref) => gen.mkAttributedIdent(ref)
      case _              => super.transform(tree)
    }
  }

  object Forwarded {
    // A map of symbol to its forwardee. If you type "val x = y" in a forwarder trait then this will contain x -> Some(y).
    private val _forwarder = perRunCaches.newAnyRefMap[Symbol, Option[Symbol]]()
    def unapply(tree: Tree): Option[Symbol] = tree match {
      case ref: RefTree if ref.symbol.isFinal =>
        val sym = ref.symbol
        _forwarder.getOrElseUpdate(sym, sym.info.resultType match {
          case SingleType(_, ref) if sym.owner hasAnnotation ForwarderAttr => Some(ref)
          case _                                                           => None
        })
      case _ => None
    }
  }

  lazy val ForwarderAttr = rootMirror.requiredClass[forwarder]
}
