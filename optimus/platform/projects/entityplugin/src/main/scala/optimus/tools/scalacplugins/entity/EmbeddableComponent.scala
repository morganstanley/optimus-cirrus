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

import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.reporter.OptimusPluginReporter

import scala.tools.nsc._
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.symtab.Flags
import scala.tools.nsc.transform._
import Flags._

/**
 * for @embeddable case object, we do following: generate pickle method rewrite the apply method for companion object of
 * \@node embeddable class generate the readResolve method for companion object of @node embeddable class
 */
class EmbeddableComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with Transform
    with TransformGrep
    with TypingTransformers
    with TypedUtils
    with OptimusPluginReporter
    with TreeDuplicator
    with ast.TreeDSL
    with WithOptimusPhase {
  import global._, CODE._

  def newTransformer0(unit: CompilationUnit) = new EmbeddableTransformer(unit)

  /**
   * TODO (OPTIMUS-0000): Has bugs, as object entity extends someEntity is really broken
   */
  class EmbeddableTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {

    /** Ideally we should use @intern on the class itself */
    private def shouldInternValues(module: Symbol): Boolean = {
      val primaryCtorOfClass = module.companionClass.primaryConstructor
      primaryCtorOfClass.hasAnnotation(NodeAnnotation)
    }

    private def markEmbeddableAsInterning(module: Symbol): Tree = {
      localTyper.typedPos(module.pos) { q"$module.__intern()" }
    }

    private def wrapInIntern(dd: DefDef): Tree = {
      val owner = dd.symbol.owner
      val companionObj = owner.companionModule.moduleClass
      if (shouldInternValues(owner)) {
        deriveDefDef(dd) { rhs =>
          atOwner(dd.symbol) { localTyper.typedPos(dd.pos) { q"$companionObj.this.__intern($rhs)" } }
        }
      } else super.transform(dd)
    }

    override def transform(tree: Tree): Tree = tree match {
      case clz: ClassDef if clz.symbol.isCaseClass && (isEmbeddable(clz.symbol) || isStable(clz.symbol)) =>
        val newClass = handleClassDef(tree, clz)
        super.transform(newClass)

      case impl: ImplDef if impl.symbol.baseClasses.exists(isEmbeddable) && !isEmbeddable(impl.symbol) =>
        alarm(
          OptimusErrors.EMBEDDABLE_SUBTYPE,
          impl.pos,
          impl.symbol.baseClasses.find(isEmbeddable).get.name.toString,
          impl.name.toString)
        super.transform(impl)

      case md: ModuleDef if shouldInternValues(md.symbol) =>
        val newModDef = deriveModuleDef(md)(deriveTemplate(_) { body =>
          markEmbeddableAsInterning(md.symbol) :: body
        })
        super.transform(newModDef)

      case dd @ DefDef(_, nme.hashCode_, _, List(Nil), _, _) if dd.symbol.owner.hasAttachment[StableAttachment] =>
        buildHashCode(dd)

      case dd @ DefDef(_, nme.equals_, Nil, List(List(param: ValDef)), _, _)
          if dd.symbol.owner.hasAttachment[StableAttachment] =>
        buildEquals(dd, param)

      case dd @ DefDef(_, names.apply, _, _, _, _)
          if dd.symbol.owner.companion.isCaseClass && isEmbeddable(dd.symbol.owner.companion) =>
        wrapInIntern(dd)
      case dd @ DefDef(_, names.copy, _, _, _, _) if dd.symbol.owner.isCaseClass && isEmbeddable(dd.symbol.owner) =>
        wrapInIntern(dd)
      case _ => super.transform(tree)
    }

    private def handleClassDef(tree: global.Tree, clz: global.ClassDef) = {
      // unfortunately the annotations on the params don't make it into the accessor methods
      val ignoredParams: Set[String] =
        clz.symbol.asClass.primaryConstructor.paramss.head.collect {
          case p if p.hasAnnotation(NotPartOfIdentityAnnotation) => p.nameString
        }.toSet

      val params = clz.symbol.info.decls.iterator.collect {
        case m
            if m.isCaseAccessorMethod
              && !ignoredParams(m.nameString) =>
          m.asMethod
      }.toList

      val stable: StableAttachment = new StableAttachment(isEmbeddable(clz.symbol), isStable(clz.symbol), params)
      var okToProceed = true

      if (!useOptimusHashCodeMechanismsForEmbeddable && stable.embeddable && !stable.stable && ignoredParams.nonEmpty) {
        alarm(OptimusErrors.EMBBEDDABLE_WITH_IGNORED, clz.pos)
        okToProceed = false
      }

      // for the moment we will allow @stable @embeddable to ease the migration
      // when the migration is complete we may consider if all @embeddabes are @stable
//
//      //its bad, but will not affect the rewrite, so we can leave okToProceed alone
//      if (stable.embeddable && isStable(clz.symbol))
//        alarm(OptimusErrors.NOT_STABLE_AND_EMBBEDDABLE, clz.pos)

      // its bad, but will not affect the rewrite, so we can leave okToProceed alone
      // are all of the parameters ignored?
      if (params.isEmpty) {
        if (stable.embeddable)
          alarm(OptimusErrors.EMBEDDABLE_WITHOUT_PARAMS, clz.pos)
        if (stable.stable)
          alarm(OptimusErrors.STABLE_WITHOUT_PARAMS, clz.pos)
      }

      // make sure that we have an equals and hashcode - e.g. not inherited from a class with a final one
      // we can check the details of the methods when we traverse them, but we need to ensure that they are there
      clz.symbol.info.decl(nme.equals_).alternatives.find { x =>
        val params = x.info.paramss
        params.size == 1 && params.head.size == 1 && params.head.head.tpe == definitions.AnyTpe
      } match {
        case None =>
          okToProceed = false
          if (stable.embeddable)
            alarm(OptimusErrors.NO_METHOD_IN_EMBEDDABLE_CASE_CLASS, clz.pos, "equals")
          if (stable.stable)
            alarm(OptimusErrors.NO_METHOD_IN_STABLE_CASE_CLASS, clz.pos, "equals")
        case Some(sym) =>
          if (!sym.isSynthetic) {
            okToProceed = false
            if (stable.embeddable)
              alarm(OptimusErrors.NO_CUSTOM_METHOD_IN_EMBEDDABLE_CASE_CLASS, clz.pos, nme.equals_)
            if (stable.stable)
              alarm(OptimusErrors.NO_CUSTOM_METHOD_IN_STABLE_CASE_CLASS, clz.pos, nme.equals_)
          }
      }

      clz.symbol.info.decl(nme.hashCode_).alternatives.find { _.paramss == List(Nil) } match {
        case None =>
          okToProceed = false
          if (stable.embeddable)
            alarm(OptimusErrors.NO_METHOD_IN_EMBEDDABLE_CASE_CLASS, clz.pos, "hashCode")
          if (stable.stable)
            alarm(OptimusErrors.NO_METHOD_IN_STABLE_CASE_CLASS, clz.pos, "hashCode")
        case Some(sym) =>
          if (!sym.isSynthetic) {
            if (stable.embeddable)
              alarm(OptimusErrors.NO_CUSTOM_METHOD_IN_EMBEDDABLE_CASE_CLASS, clz.pos, nme.hashCode_)
            if (stable.stable)
              alarm(OptimusErrors.NO_CUSTOM_METHOD_IN_STABLE_CASE_CLASS, clz.pos, nme.hashCode)
          }
      }
      // There will be some edge cases where there is a child class if it overrides hashcode or equals
      // and we can't just make the method final not as the typechecker will not have to run on the child class
      // and the message would be confusing anyway
      // if safe if th class is final, or at least there ar no subclasses
      // we allow final case class ... or sealed case class ...., but only if its sealed and there are no chlld classes
      // the sealed form is to allow us to get around mocking, which requires the class is non final
      // but we can allow mocking to use this back door, and still have compile time safety
      if (!clz.symbol.isFinal && !(clz.symbol.isSealed && clz.symbol.knownDirectSubclasses.isEmpty)) {
        okToProceed = false
        if (stable.embeddable)
          alarm(OptimusErrors.EMBEDDABLE_CASE_CLASS_NOT_FINAL, clz.pos)
        if (stable.stable)
          alarm(OptimusErrors.STABLE_CASE_CLASS_NOT_FINAL, clz.pos)
      }
      val nonObjectOwner =
        clz.symbol.owner.ownersIterator.dropWhile(o => o.isModuleOrModuleClass && !o.hasPackageFlag).next()
      if (!nonObjectOwner.hasPackageFlag) {
        okToProceed = false
        if (stable.embeddable)
          alarm(OptimusErrors.EMBEDDABLE_CASE_CLASS_HAS_OUTER, clz.pos, nonObjectOwner.fullName)
        if (stable.stable)
          alarm(OptimusErrors.STABLE_CASE_CLASS_HAS_OUTER, clz.pos, nonObjectOwner.fullName)
      }
      // if the class doesn't fulfil the basic requirements that we need, then don't mark it with the
      // attachment, as that will ensure that we don't attempt the equals/hashcode rewrite
      // its really all or nothing, and for nothing the warning.errors will be generated
      if (okToProceed) {
        val (hashcodeSymbol, hashcodeValidSymbol) =
          if (params.isEmpty) (null, null)
          else {
            val hashValue = clz.symbol
              .newVariable(names.cachedHashcode, clz.pos.focus, SYNTHETIC | PRIVATE)
              .setInfoAndEnter(definitions.IntTpe)
            hashValue.addAnnotation(AnnotationInfo(definitions.TransientAttr.tpe, Nil, Nil))

            val singleField = useOptimusHashCodeMechanismsForEmbeddable || stable.stable

            if (singleField)
              hashValue.addAnnotation(AnnotationInfo(definitions.VolatileAttr.tpe, Nil, Nil))

            val hashValid = if (!singleField) {
              val hashValid = clz.symbol
                .newVariable(names.cachedHashcodeValid, clz.pos.focus, SYNTHETIC | PRIVATE)
                .setInfoAndEnter(definitions.BooleanTpe)
              hashValid.addAnnotation(AnnotationInfo(definitions.TransientAttr.tpe, Nil, Nil))
              hashValid.addAnnotation(AnnotationInfo(definitions.VolatileAttr.tpe, Nil, Nil))
            } else null

            (hashValue, hashValid)
          }
        stable.hashcode = hashcodeSymbol
        stable.hashcodeValid = hashcodeValidSymbol

        clz.symbol.updateAttachment(stable)
      }
      var result = clz
      if (isEmbeddable(clz.symbol)) {
        val hasNodeCtor = clz.impl.body.exists {
          case dd: DefDef => dd.name == nme.CONSTRUCTOR && hasNodeAnnotation(dd)
          case _          => false
        }
        if (hasNodeCtor) {
          if (clz.symbol.info.decl(nme.readResolve).isMethod)
            alarm(OptimusErrors.NO_READRESOLVE_OVERRIDDEN_IS_ALLOWED_IN_NODE_EMBEDDABLE, clz.pos, clz)
          val readResolve = mkReadResolve(clz)
          result = deriveClassDef(clz)(deriveTemplate(_)(_ :+ readResolve))
        }
      }
      result
    }
    private def buildEquals(dd: global.DefDef, param: ValDef) = try {
      val ownerSymbol = dd.symbol.owner
      val stable = ownerSymbol.attachments.get[StableAttachment].get
      val res = deriveDefDef(dd) { _ =>
        if (stable.hasParams) {
          val newMethod = localTyper.typedPos(dd.pos) {

            // we order the checks that we apply in increasing cost
            // 1 - primitive values - they are really cheap
            // 2 - String - very common and should be cheap
            // 3 - @embeddable, @stable @entity and @event - should all have fast hashcode and equals
            // 4 - the rest
            def order(param: (global.Symbol, RunId)): RunId = {
              val (sym, pos) = param
              val tpe = sym.tpe
              val complex =
                if (tpe.dealias.typeSymbol.isPrimitiveValueClass) 0
                else if (tpe =:= typeOf[String]) 1
                else if (isEmbeddable(sym) || isStable(sym) || isEntityOrEvent(sym)) 2
                else 3
              complex * 1000 + pos
            }

            val fields = stable.params.zipWithIndex.sortBy(order).map(_._1)

            val that = dd.symbol
              .newVariable(freshTermName("equals$that")(unit.fresh), dd.pos.focus) setInfo ownerSymbol.tpe
            val other = newValDef(that, REF(param.symbol) AS ownerSymbol.tpe)()

            val identical: Tree = This(ownerSymbol) OBJ_EQ Ident(that)
            val hashEquals = Apply(
              PluginSupport.equalsAvoidingZero,
              List[Tree](Select(This(ownerSymbol), stable.hashcode), Select(Ident(that), stable.hashcode)))
            val quickChecks = fields map {
              case param if param.tpe.dealias.typeSymbol.isPrimitiveValueClass =>
                Apply(PluginSupport.equals, List[Tree](Select(This(ownerSymbol), param), Select(Ident(that), param)))
              case param =>
                val method =
                  if (isEntityOrEvent(param)) PluginSupport.canEqualStorable
                  else PluginSupport.canEqual
                Apply(method, List[Tree](Select(This(ownerSymbol), param), Select(Ident(that), param)))
            }
            val deepChecks = fields collect {
              case param if !param.tpe.dealias.typeSymbol.isPrimitiveValueClass =>
                Apply(PluginSupport.equals, List[Tree](Select(This(ownerSymbol), param), Select(Ident(that), param)))
            }
            val calcEquals = Block(List(other), identical OR AND(hashEquals :: quickChecks ::: deepChecks: _*))

            BLOCK(
              IF(REF(param.symbol) IS_OBJ ownerSymbol.tpe) THEN {
                calcEquals
              } ELSE FALSE
            )
          }
          newMethod
        } else {
          localTyper.typedPos(dd.pos)(REF(param.symbol) IS_OBJ ownerSymbol.tpe)
        }
      }
      // it would be nice to be able to make this final
      // but we can only do this for final classes - where the mocking doesnt write its own hashcode and equals
      // based on identity
      // i.e.
      // .copy(mods = mods | Flags.FINAL)
      // dd.symbol.flags = dd.symbol.flags | Flags.FINAL
      res.setSymbol(dd.symbol)
      res.setType(dd.tpe)
      res.setAttachments(dd.attachments)
      res
    } catch {
      case ex: Throwable =>
        global.reporter.error(dd.symbol.pos, scala.tools.nsc.util.stackTraceString(ex))
        dd
    }
    // for the moment we will use the scala semantics for hashcode
    // it would be better to migrate to optimus so we leave the code for that
    // TODO (OPTIMUS-31928): should be true
    private def useOptimusHashCodeMechanismsForEmbeddable = false
    private def buildHashCode(dd: global.DefDef): DefDef = {
      val stable = dd.symbol.owner.attachments.get[StableAttachment].get
      val res = deriveDefDef(dd) { rhs =>
        if (useOptimusHashCodeMechanismsForEmbeddable || stable.stable) {
          if (stable.hasParams) {
            val newMethod = localTyper.typedPos(dd.pos) {
              val statements = List.newBuilder[Tree]
              val acc = dd.symbol
                .newVariable(freshTermName("hashcode$acc")(unit.fresh), dd.pos.focus) setInfo definitions.IntTpe
              statements += newValDef(acc, Literal(Constant(dd.symbol.fullName.hashCode)))()
              stable.params foreach { param =>
                statements += Assign(Ident(acc), Apply(PluginSupport.hashOf, List[Tree](Ident(acc), Ident(param))))
              }
              val calcHashcode =
                Block(
                  statements.result(),
                  Assign(REF(stable.hashcode), Apply(PluginSupport.avoidZero, List(Ident(acc)))))

              BLOCK(
                IF(REF(stable.hashcode) INT_== LIT(0)) THEN {
                  calcHashcode
                } ENDIF,
                REF(stable.hashcode)
              )
            }
            newMethod
          } else {
            localTyper.typedPos(dd.pos)(Literal(Constant(dd.symbol.fullName.hashCode)))
          }
        }
        // TODO (OPTIMUS-31928): should be deleted. We should always use optimus semantics
        else {
          if (stable.hasParams) {
            val newMethod = localTyper.typedPos(dd.pos) {
              val calcHashcode =
                Block(
                  List(Assign(REF(stable.hashcode), rhs)),
                  Assign(REF(stable.hashcodeValid), Literal(Constant(true))))

              BLOCK(
                IF(NOT(REF(stable.hashcodeValid))) THEN {
                  calcHashcode
                } ENDIF,
                REF(stable.hashcode)
              )
            }
            newMethod
          } else {
            rhs
          }
        }
      }
      // it would be nice to be able to make this final
      // but we can only do this for final classes - where the mocking doesnt write its own hashcode and equals
      // based on identity
      // i.e.
      // .copy(mods = mods | Flags.FINAL)
      // dd.symbol.flags = dd.symbol.flags | Flags.FINAL
      res.setSymbol(dd.symbol)
      res.setType(dd.tpe)
      res.setAttachments(dd.attachments)
      res
    }

    private def mkReadResolve(cd: ClassDef): Tree = {
      val cls = cd.symbol
      localTyper.typedPos(cd.pos) {
        val readResolveMethod = cls.newMethod(nme.readResolve, cd.pos).setFlag(SYNTHETIC | FINAL)
        readResolveMethod.setInfoAndEnter(NullaryMethodType(definitions.AnyRefTpe))
        DefDef(readResolveMethod, q"${cls.companionModule}.__intern($cls.this)")
      }
    }

    def asString(t: Any, prefix: String = "\n"): String = {
      t match {
        case l: List[_] =>
          s"List($prefix${l.map(x => asString(x, prefix + "  ")).mkString(s",$prefix")}$prefix)"
        case p: Product =>
          s"${p.productPrefix}($prefix${p.productIterator.map(x => asString(x, prefix + "  ")).mkString(s",$prefix")}$prefix)"
        case x if x.isInstanceOf[TermName] =>
          val name = x.toString
          s"""TermName("$name")"""
        case x if x.isInstanceOf[TypeName] =>
          val name = x.toString
          s"""TypeName("$name")"""
        case x: String           => s""""$x""""
        case x: java.lang.Number => x.toString
        case x: Boolean          => x.toString
        case _                   => s" ***** ${t.getClass} - $t"
      }
    }
    private class StableAttachment(val embeddable: Boolean, val stable: Boolean, val params: List[MethodSymbol]) {
      // set to a symbol if attached to the class symbol
      // its the symbol for the local cache of the hashcode, so we only generate it if we need the field, ie
      // if we are doing the rewrite
      var hashcode: TermSymbol = _
      var hashcodeValid: TermSymbol = _
      def hasParams: Boolean = params.nonEmpty
    }

  }
}
