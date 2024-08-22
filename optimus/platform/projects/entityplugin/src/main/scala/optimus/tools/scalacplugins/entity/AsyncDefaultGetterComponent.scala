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

import scala.tools.nsc.Global

/**
 * AsyncDefaultGetterComponent identifies default parameters (eg. `bar` in `def foo(i: Int = bar)`) that make async
 * calls, and tags the scalac-generated `foo$default$1` method with a Node attachment so that the
 * GenerateNodeMethodsComponent later will treat that as an @node and perform the standard node transformations to turn
 * it into an async method.
 */
class AsyncDefaultGetterComponent[G <: Global](val pluginData: EntityPluginData, val global: G)
    extends provider.PluginProvider[G]
    with TypedUtils
    with NodeClassification { self =>
  import global._

  override val phaseInfo: OptimusPhaseInfo = OptimusPhaseInfo.NoPhase

  override def entitySettings: EntitySettings = pluginData.settings
  override def analyzerPlugins: List[analyzer.AnalyzerPlugin] = asyncDefaultGetterTyper :: Nil

  object asyncDefaultGetterTyper extends analyzer.AnalyzerPlugin {
    import analyzer._

    override def isActive(): Boolean = !isPastTyper

    override def pluginsTypeSig(tpe: Type, typer: Typer, defTree: Tree, pt: Type): Type = defTree match {
      case dd: DefDef if dd.symbol.isDefaultGetter =>
        val sym = dd.symbol
        val status = AsyncCallCollector(typer.typed(dd.rhs.duplicate, typer.typedType(dd.tpt).tpe))
        if (!status.isAsync) {
          overriddenNode(sym) foreach { overridden =>
            makeAsync(dd, allowedInSIContext(overridden), !isImpure(overridden))
          }
        } else {
          makeAsync(dd, !status.accessesTweaks, !status.accessesImpure)
        }
        tpe
      case _ => tpe
    }
  }

  private def isAsync(tree: Tree) =
    hasNodeAnnotation(tree) || hasNodeSyncAnnotation(tree) || hasAsyncAnnotation(tree)
  private def isImpure(sym: Symbol) =
    sym.hasAnnotation(ImpureAnnotation) || sym.hasAnnotation(AspirationallyImpureAnnotation)

  private object AsyncCallCollector extends Traverser {
    case class Result(isAsync: Boolean, accessesTweaks: Boolean, accessesImpure: Boolean) {
      def &&(other: Result): Result = Result(
        isAsync = this.isAsync || other.isAsync,
        accessesTweaks = this.accessesTweaks || other.accessesTweaks,
        accessesImpure = this.accessesImpure || other.accessesImpure
      )
    }

    var result: Result = _

    override def traverse(tree: Tree): Unit = {
      if (tree.hasSymbolField) {
        val async = isAsync(tree)
        // functions which aren't async are always considered SI
        val accessesTweak = async && (!allowedInSIContext(tree.symbol))
        val impure = isImpure(tree.symbol) // however, purity is not dependent on async-ness
        result &&= Result(async, accessesTweak, impure)
      }

      // traverse but avoiding by name params of non SI transparent functions
      tree match {
        case Apply(f, _) if f.symbol.isMethod && !isSITransparent(f.symbol) =>
          val treeInfo.Applied(fun, _, argss) = tree
          traverse(fun)
          // only traverse args which are NOT byName/function/nodeLifted because those aren't necessarily
          // run under the current scope
          foreach2(argss, fun.symbol.asMethod.paramss) { (args, params) =>
            foreach2(args, params) { (arg, param) =>
              if (
                !param.isByNameParam && !definitions.isFunctionType(param.tpe) &&
                !hasNodeLiftAnno(param)
              ) traverse(arg)
            }
          }
        case _ => super.traverse(tree)
      }

    }

    def apply(tree: Tree): Result = {
      result = Result(isAsync = false, accessesTweaks = false, accessesImpure = false)
      traverse(tree)
      result
    }
  }

  def overriddenNode(method: Symbol): Option[Symbol] =
    if (method.owner.isTerm) None
    else {
      val name = method.name
      method.owner.ancestors.collectFirst {
        case a if a.hasRawInfo && a.rawInfo.decls.containsName(name) =>
          val overridden = a.rawInfo.decl(name)
          if (isNode(overridden.initialize)) Some(overridden) else None
      }.flatten
    }

  def makeAsync(dd: DefDef, isSI: Boolean, isRT: Boolean): Unit = {
    val sym = dd.symbol
    val nodeInfo = Attachment.Node(
      if (isRT) Attachment.NodeType.RawNode(scenarioIndependent = isSI, createNodeTrait = false, notTweakable = true)
      else Attachment.NodeType.NonRTNode(scenarioIndependent = isSI, createNodeTrait = false),
      async = true
    )
    sym.updateAttachment(nodeInfo)
    addNodeAnnotations(sym, nodeInfo)
    if (!isRT) sym.addAnnotation(ImpureAnnotation)
  }

}
