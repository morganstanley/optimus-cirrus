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
import scala.tools.nsc.symtab.Flags
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors

trait NodeSynthesis { this: UntypedUtils =>

  val global: Global
  import global._

  import Flags._

  def checkIllegalDefAnnots(dd: DefDef, implDef: ImplDef): Unit = {
    val mods = dd.mods

    if (hasAnnotation(mods, tpnames.job)) {
      // limit @job to be defined on non-entity object [[JOB_EXPERIMENTAL]]
      if (!isNonEntityObject(implDef)) alarm(OptimusErrors.JOB_ENTITY_OR_NONOBJECT, dd.pos)
      // enforce @job requires @node / @async [[JOB_EXPERIMENTAL]]
      if (!hasNodeOrAsyncAnno(dd)) alarm(OptimusErrors.JOB_WITHOUT_NODE_ASYNC, dd.pos)
    }
    if (hasAnnotation(mods, tpnames.stored))
      alarm(OptimusErrors.STORED_WITH_DEF, dd.pos)
  }

  // Globally-accessible transforms (common to entities and non-entity classes)
  protected def transformImpure(dd: DefDef): DefDef = dd match {
    case dd @ DefDef(mods, name, tparams, vparamss, tpt, rhs)
        if hasAnnotation(mods, tpnames.impure) && (name != nme.CONSTRUCTOR) && !mods.hasFlag(Flags.SYNTHETIC) =>
      if (hasAnnotation(mods, tpnames.handle)) alarm(OptimusErrors.IMPURE_WITH_HANDLER, dd.pos)
      if (hasAnnotation(mods, tpnames.entersGraph)) alarm(OptimusErrors.IMPURE_WITH_ENTERSGRAPH, dd.pos)

      // (OPTIMUS-11524) if no definition, we can't add verifyImpure call in the rhs, but the compile time check should work for this abstract method
      // but the inheritance annotation check will ensure all impls are @impure as well, which means they will do runtime impure check
      if (dd.mods.isDeferred || rhs.isEmpty) dd
      else treeCopy.DefDef(dd, mods, name, tparams, vparamss, tpt, mkImpureBlock(rhs))
    case _ => dd
  }

  def reportAsyncValError(vd: ValDef): Unit = {
    alarm(OptimusErrors.ASYNC_WITH_VAL, vd.pos)
  }

  def checkNodeWithoutTweakProp(dd: DefDef): Unit = {
    if (hasTweak(dd.mods)) alarm(OptimusErrors.TWEAKNODE_IN_NON_ENTITY, dd.pos)
  }

  def transformRawNodeDef(tree: Tree): List[Tree] = transformRawNodeDef(tree, null)
  def transformRawNodeDef(
      tree: Tree,
      onAsyncDefDef: DefDef => Tree = null,
      classMods: Modifiers = null,
      cmdef: ImplDef = null
  ): List[Tree] = {

    tree match {
      case vd @ ValDef(mods, _, _, _) if hasAnnotation(mods, tpnames.job) => // [[JOB_EXPERIMENTAL]]
        alarm(OptimusErrors.JOB_WITH_VAL, vd.pos)
      case d: ValOrDefDef =>
        if (hasAnnotation(d.mods, tpnames.givenRuntimeEnv))
          alarm(OptimusErrors.ILLEGAL_ANNOTATION, tree.pos, tpnames.givenRuntimeEnv, "on non-property members")
      case _ =>
    }

    tree match {
      case vd @ ValDef(mods, name, tpt, rhs)
          if hasAnnotation(vd.mods, tpnames.async) && hasAnnotation(vd.mods, tpnames.givenAnyRuntimeEnv) =>
        if (mods.isDeferred)
          alarm(OptimusErrors.NO_DEFERRED_LAZY_VALS, vd.pos)
        // Create container val foo$asyncLazy = asyncLazy { rhs }
        val lazyHolderName = newTermName("" + name + suffixes.ASYNCLAZY)
        // Ensure that the container is initialized when we access it, even if that occurs from supertrait
        // construction, by adding the LAZY flag.
        val queuedMods = withoutAnnotations(mods, tpnames.async :: tpnames.givenAnyRuntimeEnv :: Nil) & ~Flags.LAZY
        val lazyHolderMods = (queuedMods | Flags.LAZY | Flags.PrivateLocal) & ~Flags.OVERRIDE & ~Flags.PROTECTED
        val lazyHolderType = tpt match {
          case TypeTree() => TypeTree()
          case _          => AppliedTypeTree(AsyncLazyType, List(dupTree(tpt) setPos NoPosition))
        }
        val lazyHolderRhs = q"optimus.platform.asyncLazyWithAnyRuntimeEnv { ${dupTree(rhs)} }"
        val lazyHolderValDef = atPos(tree.pos.focus) {
          ValDef(lazyHolderMods, lazyHolderName, lazyHolderType, lazyHolderRhs)
        }

        val syncMods =
          queuedMods.withAnnotations(mkAnnotation(NodeSyncAnnotationType, tree.pos.focus) :: Nil) | Flags.SYNTHETIC

        val queuedDefDef = atPos(tree.pos.focus) {
          val rt = tpt match {
            case TypeTree() => TypeTree()
            case _          => AppliedTypeTree(NodeType, List(dupTree(tpt) setPos NoPosition))
          }
          DefDef(queuedMods, mkGetNodeName(name), Nil, Nil, rt, q"$lazyHolderName.${newTermName("deref$queued")}")
        }
        val getterDefDef = atPos(tree.pos.focus) {
          val syncRhs = q"$lazyHolderName.${newTermName("deref")}"
          DefDef(syncMods, name.toTermName, Nil, Nil, dupTree(tpt) setPos NoPosition, syncRhs)
        }

        lazyHolderValDef :: queuedDefDef :: getterDefDef :: Nil

      case dd: ValOrDefDef if hasAnnotation(dd.mods, tpnames.givenAnyRuntimeEnv) =>
        alarm(OptimusErrors.GIVEN_RUNTIME_ENV_MISUSE, dd.pos)
        dd :: Nil

      case dd: ValOrDefDef if hasAnnotation(dd.mods, tpnames.backed) =>
        alarm(OptimusErrors.BACKED_IN_NON_ENTITY, dd.pos)
        dd :: Nil

      case vd: ValDef => // checks for the val defined in non-entity types
        if (hasAnnotation(vd.mods, tpnames.async) && !hasAnnotation(vd.mods, tpnames.givenAnyRuntimeEnv))
          reportAsyncValError(vd)
        if (hasAnnotation(vd.mods, tpnames.node)) alarm(OptimusErrors.NONENTITY_NODE_WITH_VAL, vd.pos)
        if (hasAnnotation(vd.mods, tpnames.impure)) alarm(OptimusErrors.IMPURE_WITH_VAL, vd.pos)
        vd :: Nil
      case dd @ DefDef(mods, name, _, _, _, _)
          if hasNodeAnno(mods) && name == nme.CONSTRUCTOR && hasAnnotation(classMods, tpnames.embeddable) =>
        // always set the constructor to private when @node is used in constructor
        val newMods = dd.mods | PRIVATE
        val privateConstructor = dd.copy(mods = newMods) setPos dd.pos
        privateConstructor :: Nil
      case dd @ DefDef(mods, name, _, _, _, _)
          if (name != nme.CONSTRUCTOR) &&
            (hasAnyAnnotation(mods, Seq(tpnames.async, tpnames.elevated)) || hasNodeAnno(mods)) =>
        checkIllegalDefAnnots(dd, cmdef)
        if (hasNodeAnno(mods))
          checkNodeWithoutTweakProp(dd)

        val newDD = transformImpure(dd)
        if (
          (onAsyncDefDef ne null) && !mods.isDeferred &&
          (hasExposeArgTypes(mods) || hasAnnotation(mods, tpnames.elevated))
        ) {
          // Add generic NodeTaskInfo to entity companion so we can install plugin and return new node trait
          onAsyncDefDef(dd) :: newDD :: Nil
        } else newDD :: Nil
      case dd: DefDef =>
        checkIllegalDefAnnots(dd, cmdef)
        transformImpure(dd) :: Nil
      case _ => tree :: Nil
    }
  }

}
