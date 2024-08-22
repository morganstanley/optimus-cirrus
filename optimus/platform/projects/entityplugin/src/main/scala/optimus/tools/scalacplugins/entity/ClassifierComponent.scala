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

import optimus.tools.scalacplugins.entity.reporter.OptimusAlarmBuilder1
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors

import scala.reflect.internal.Flags._
import scala.reflect.internal.util.TriState
import scala.tools.nsc._
import scala.tools.nsc.transform.InfoTransform

class ClassifierComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with InfoTransform
    with SafeTransform
    with TransformGrep
    with TypedUtils
    with NodeClassification
    with WithOptimusPhase {
  import global._

  override def newTransformer0(unit: CompilationUnit) = new ClassifierTransformer()

  override protected val transformError: OptimusAlarmBuilder1 = OptimusErrors.CLASSIFIER_TRANSFORM_ERROR

  // Deliberately exclude embeddables here, since we never rewrite blocking properties to be lazy on embeddables
  private def stored(sym: Symbol) = !sym.owner.isModuleClass && (
    (sym.owner.hasAnnotation(EventAnnotation) && sym.name != names.validTime) ||
      (sym.owner.hasAnnotation(EntityAnnotation) && sym.owner.hasAnnotation(StoredAnnotation)) ||
      sym.hasAnnotation(StoredAnnotation)
  )

  private def nodeLike(sym: Symbol, stored: Boolean): Boolean =
    sym.hasAnnotation(NodeSyncAnnotation) || sym.hasAnnotation(NodeAnnotation) ||
      (sym.isSourceMethod && !sym.hasAnnotation(ValAccessorAnnotation) &&
        (sym.hasAnnotation(AsyncAnnotation) || sym.hasAnnotation(ElevatedAnnotation))) ||
      (stored && isBlockingProperty(sym))

  override def transformInfo(sym: Symbol, info: Type): Type = {
    sym match {
      case sym: MethodSymbol if !currentRun.compiles(sym) || sym.isStructuralRefinementMember =>
        val isStored = sym.hasAnnotation(ValAccessorAnnotation) && stored(sym)
        if (nodeLike(sym, isStored))
          sym.updateAttachment(nodeAttachment(sym))
      case _ =>
    }

    info
  }

  class ClassifierTransformer extends SafeTransformer {
    import global._

    override def transformSafe(tree: Tree): Tree = tree match {
      case dd: DefDef if !dd.symbol.isGetter && !dd.symbol.isConstructor && !dd.symbol.isLocalToBlock =>
        val sym = dd.symbol
        if (nodeLike(sym, stored = false)) {
          val nodeInfo = nodeAttachment(sym)
          sym.updateAttachment(nodeInfo)
          sym.updateAttachment(Attachment.NodeFieldType.PlainDef())
          addNodeAnnotations(sym, nodeInfo)
          super.transformSafe(deriveDefDef(dd)(wrapAsByname))
        } else {
          super.transformSafe(dd)
        }

      case vd: ValDef if !vd.symbol.isLocalToBlock => // val which may also have a typer-generated getter def
        // for vals with getters, the annotations exist on the getter
        val valSym = vd.symbol
        val sym = if (valSym.isGetter) valSym else valSym.getterIn(valSym.owner)

        // OPTIMUS-65855. Clear the `alias` for param accessors of entity classes.
        // The scala compiler uses parameter aliases for eliminating unnecessary fields.
        // The entity plugin breaks certain assumptions when changing the class structure of stored entities.
        if (
          valSym.isParamAccessor &&
          (valSym.owner.hasAnnotation(EntityAnnotation) || valSym.owner.hasAnnotation(EventAnnotation))
        ) {
          List(valSym, sym).foreach {
            case s: TermSymbol if stored(s.referenced) =>
              s.referenced = NoSymbol
            case _ =>
          }
        }

        val isStored = stored(sym)
        val isNodeLike = nodeLike(sym, isStored)

        if (isStored) {
          // TODO (OPTIMUS-25674): this special dispensation for events is a workaround
          val isLazyLoaded = isBlockingProperty(sym) && !isEvent(valSym.owner)
          // TODO (OPTIMUS-26094): We should just have one Stored annotation (rather than StoredGetter and StoredBackingVal),
          // rename NodeFieldType to OptimusFieldType or FieldType, and use (Optimus)FieldType for any vals/defs
          // that optimus might be interested in (ie. stored or node).
          sym.updateAttachment(Attachment.StoredGetter(isLazyLoaded))
          if (!sym.isVal) {
            val backingVal = sym.accessed
            backingVal.updateAttachment(Attachment.StoredBackingVal(sym, isLazyLoaded))
          }
        }

        if (isNodeLike || isStored) sym.addAnnotation(ValAccessorAnnotation)

        if (isNodeLike) {
          val nodeInfo = nodeAttachment(sym)
          sym.updateAttachment(nodeInfo)
          addNodeAnnotations(sym, nodeInfo)
          if (sym == valSym) {
            sym.updateAttachment(Attachment.NodeFieldType.BareVal())
          } else {
            // sym is an accessor
            sym.updateAttachment(Attachment.NodeFieldType.GetterDef(valSym))
            // also put the node info on the backing val, since that's actually the tree we're going to
            // transform in GenerateNodeMethodsComponent.
            valSym.updateAttachment(nodeInfo)
            valSym.updateAttachment(Attachment.NodeFieldType.BackingVal(sym))
          }
          super.transformSafe(deriveValDef(vd)(wrapAsByname))
        } else {
          super.transformSafe(vd)
        }

      case _ =>
        super.transformSafe(tree)
    }
  }

  def nodeAttachment(sym: Symbol): Attachment.Node = {
    val rt = !sym.hasAnnotation(ImpureAnnotation) && !sym.hasAnnotation(AsyncAnnotation) &&
      !sym.hasAnnotation(ElevatedAnnotation)
    val raw = !sym.owner.hasAnnotation(EntityAnnotation) || sym.isDefaultGetter
    val createNodeTrait = hasExposeArgTypes(sym) || sym.hasAnnotation(ElevatedAnnotation)
    // Non-@node methods are not tweakable, and can't be overridden to be tweakable
    val tweakable = toTriState(hasTweakableOption(sym)) match {
      case TriState.Unknown =>
        if (sym.hasAnnotation(TweakableAnnotation)) TriState.True
        else if (sym.hasAnnotation(NonTweakableAnnotation)) TriState.False
        else TriState.Unknown
      case known => known
    }

    val isVal = sym.isGetter || sym.hasAnnotation(ValAccessorAnnotation)
    val si = sym.hasAnnotation(ScenarioIndependentAnnotation) || (isVal && !sym.hasAnnotation(NodeAnnotation))

    val async = !sym.hasAnnotation(AsyncOffAnnotation)

    val nodeType =
      if (!rt) Attachment.NodeType.NonRTNode(scenarioIndependent = si, createNodeTrait = createNodeTrait)
      else if (raw) Attachment.NodeType.RawNode(si, createNodeTrait, notTweakable = tweakable == TriState.False)
      else if (!isVal) {
        Attachment.NodeType.PropertyNode(si, tweakable)
      } else {
        // may be a ValDef or an accessor DefDef
        val isNodeVal = sym.hasAnnotation(NodeAnnotation)
        val valTweakable = if (!isNodeVal || si) TriState.False else tweakable
        Attachment.NodeType.ValNode(valTweakable)
      }

    Attachment.Node(nodeType, async)
  }
}

/**
 * Code to be shared between optimus_classifier and optimus_asyncdefault.
 */
trait NodeClassification extends TypedUtils {
  val global: Global
  import global._

  /**
   * Add annotations to this node def corresponding to such info as later runs may need.
   */
  final def addNodeAnnotations(sym: Symbol, node: Attachment.Node): Unit = {
    addIfMissing(sym, NodeSyncAnnotation)
    // change modifier of node method
    if (node.rt == TriState.False) {
      val assoc =
        if (node.nonRTWithCreateNodeTrait) List((names.exposeWithArgTypes, LiteralAnnotArg(Constant(true)))) else Nil
      addIfMissing(sym, AsyncAnnotation, assoc)
    }
    if (!node.async) addIfMissing(sym, AsyncOffAnnotation)
    if (node.scenarioIndependent) addIfMissing(sym, ScenarioIndependentInternalAnnotation)
    // @node(tweak=false) vals are stable, but everything else should be considered unstable.
    // Note that stored vals in theory shouldn't be considered stable even if non-tweakable, since they may
    // change as a result of certain sorts of reactive DAL subscriptions. However, this would break too
    // much existing code in the codebase, so we're not enforcing that here.
    node.nodeType match {
      case Attachment.NodeType.ValNode(TriState.False) =>
        sym.setFlag(STABLE)
      case Attachment.NodeType.ValNode(_) =>
        sym.resetFlag(STABLE)
      case _ =>
        sym.resetFlag(STABLE)
    }
    node.tweakable match {
      case TriState.True  => addIfMissing(sym, TweakableAnnotation)
      case TriState.False => addIfMissing(sym, NonTweakableAnnotation)
      case _              =>
    }
  }

  /* Now that generatenodemethods runs after superaccessors, scalac won't transform accesses to super within node
   * method bodies. However, since the body of the node method will be transplanted into an inner class by asyncgraph,
   * we need to have those superaccessors! Therefore, wrap the body in a silly by-name wrapper which is removed by
   * generatenodemethods: the by-name argument will induce superaccessor synthesis as required.
   *
   * Since all of the relevant bits of the superaccessor logic are private to scalac, duplicating the logic in
   * generatenodemethods is, while feasible, nontrivial and unclear.
   */
  final def wrapAsByname(body: Tree): Tree =
    if (body.isEmpty || curInfo.isLoom) body
    else {
      val wrapper = NoSymbol.newMethod(names.bynameDummy, body.pos, SYNTHETIC)
      val wrappee = wrapper.newSyntheticValueParam(appliedType(definitions.ByNameParamClass, body.tpe :: Nil))
      wrapper.setInfo(MethodType(wrappee :: Nil, body.tpe))
      Apply(Ident(wrapper) setType wrapper.tpe, body :: Nil) setType body.tpe
    }
}
