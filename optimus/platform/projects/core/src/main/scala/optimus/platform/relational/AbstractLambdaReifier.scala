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
package optimus.platform.relational

import optimus.platform.annotations.internal._reified
import optimus.platform.internal.MacroBase
import optimus.platform.relational.tree._
import optimus.scalacompat.collection.IterableOnceOps
import optimus.scalacompat.collection.MapLike
import optimus.scalacompat.collection.SetLike
import optimus.scalacompat.collection.TraversableLike
import optimus.tools.scalacplugins.entity.reporter.RelationalAlarms

import scala.collection.mutable
import scala.reflect.macros.blackbox.Context
import scala.tools.nsc.typechecker.StdAttachments

object AbstractLambdaReifier {
  private[optimus] val ReifiedTarget = "REIFIED_TARGET"
  private[optimus] val OverloadSignature = "OVERLOAD_SIGNATURE"
}

trait AbstractLambdaReifier extends MacroBase {
  import c.universe._

  def reifyOption(t: c.Tree): Option[c.Expr[LambdaElement]] = {
    val undoMacroTree = UndoMacroTransformer.undoMacro(t)
    val consts = new ConstantNominator().nominate(undoMacroTree)
    val converted = new Convertor(consts).convertOption(undoMacroTree)
    converted.map(e => c.Expr[LambdaElement](e))
  }

  def reify(t: c.Tree): c.Expr[LambdaElement] = {
    val undoMacroTree = UndoMacroTransformer.undoMacro(t)
    val consts = new ConstantNominator().nominate(undoMacroTree)
    try {
      c.Expr[LambdaElement](new Convertor(consts).convert(undoMacroTree))
    } catch {
      case ex: Throwable => abort(RelationalAlarms.CANNOT_REIFY_ERROR, ex.getMessage)
    }
  }

  protected def checkMethod(s: Symbol, encodedName: String): Unit = {}
  protected def canTranslateToMethodElement(s: Symbol): Boolean = false
  protected def isOptionOrSeqOps(s: Symbol): Boolean = {
    import Helper.CollectionSyms
    CollectionSyms.contains(s.owner) || s.overrides.exists(o => CollectionSyms.contains(o.owner))
  }

  protected def transformConstant(constant: Tree): Tree = {
    import Helper._
    q"$ElementFactoryT.constant(${c.untypecheck(constant)}, ${mkTypeInfo(constant.tpe)})"
  }

  protected def transformContains(left: Tree, right: Tree): Tree = {
    import Helper._
    val binOp = binaryOps("contains")
    q"$ElementFactoryT.makeBinary($binOp, $left, $right)"
  }

  protected def transformBinary(opEncoded: String, left: Tree, right: Tree, method: Tree): Tree = {
    import Helper._
    val binOp = binaryOps(opEncoded)
    q"$ElementFactoryT.makeBinary($binOp, $left, $right, $method)"
  }

  object Helper {
    import definitions._

    val OptionSym = typeOf[Option[_]].typeSymbol
    val SetSym = typeOf[SetLike[_, _]].typeSymbol
    val MapSym = typeOf[MapLike[_, _, Repr forSome { type Repr <: MapLike[_, _, Repr] }]].typeSymbol
    val SeqSym = typeOf[TraversableLike[_, _]].typeSymbol
    val IterOnceOpsSym = typeOf[IterableOnceOps[_, CC forSome { type CC[X] }, _]].typeSymbol
    val CollectionSyms = Set(SetSym, MapSym, OptionSym, SeqSym, IterOnceOpsSym)

    val typeInfoT = q"_root_.optimus.platform.relational.tree.typeInfo"
    val ReifiedTpe = typeOf[_reified]
    val ParameterElementTpe = typeOf[ParameterElement]
    val RelationElementTpe = typeOf[RelationElement]

    val ListT = Ident(typeOf[List.type].termSymbol)
    val MapT = Ident(typeOf[Map.type].termSymbol)
    val ElementFactoryT = Ident(typeOf[ElementFactory.type].termSymbol)
    val TypeInfoT = Ident(typeOf[TypeInfo.type].termSymbol)
    val ExpressionListElementT = Ident(typeOf[ExpressionListElement].typeSymbol)
    val BinaryOpT = Ident(typeOf[BinaryExpressionType.type].termSymbol)
    val FieldDescT = Ident(typeOf[RuntimeFieldDescriptor].typeSymbol)
    val MethodDescT = Ident(typeOf[RuntimeMethodDescriptor].typeSymbol)
    val CtorDescT = Ident(typeOf[RuntimeConstructorDescriptor].typeSymbol)
    def constElemFalseT: Tree = q"$ElementFactoryT.constant(false, $TypeInfoT.BOOLEAN)"
    def constElemTrueT: Tree = q"$ElementFactoryT.constant(true, $TypeInfoT.BOOLEAN)"

    val binaryOps: PartialFunction[String, Tree] = {
      case "equals"      => q"$BinaryOpT.EQ"
      case "$eq$eq"      => q"$BinaryOpT.EQ"
      case "$bang$eq"    => q"$BinaryOpT.NE"
      case "$greater"    => q"$BinaryOpT.GT"
      case "$greater$eq" => q"$BinaryOpT.GE"
      case "$less"       => q"$BinaryOpT.LT"
      case "$less$eq"    => q"$BinaryOpT.LE"
      case "$bar$bar"    => q"$BinaryOpT.BOOLOR"
      case "$amp$amp"    => q"$BinaryOpT.BOOLAND"
      case "contains"    => q"$BinaryOpT.ITEM_IS_IN"
      case "$plus"       => q"$BinaryOpT.PLUS"
      case "$minus"      => q"$BinaryOpT.MINUS"
      case "$times"      => q"$BinaryOpT.MUL"
      case "$div"        => q"$BinaryOpT.DIV"
      case "$percent"    => q"$BinaryOpT.MODULO"
    }

    val knownTypes = Map[Symbol, Tree](
      BooleanClass -> q"$TypeInfoT.BOOLEAN",
      IntClass -> q"$TypeInfoT.INT",
      ShortClass -> q"$TypeInfoT.SHORT",
      DoubleClass -> q"$TypeInfoT.DOUBLE",
      FloatClass -> q"$TypeInfoT.FLOAT",
      CharClass -> q"$TypeInfoT.CHAR",
      ByteClass -> q"$TypeInfoT.BYTE",
      LongClass -> q"$TypeInfoT.LONG",
      AnyClass -> q"$TypeInfoT.ANY",
      UnitClass -> q"$TypeInfoT.UNIT",
      StringClass -> q"$TypeInfoT.STRING"
    )

    def mkTypeInfo(tpe: Type): Tree = {
      import scala.reflect.internal.Types

      tpe.dealias match {
        case TypeRef(_, sym, Nil) if knownTypes.contains(sym) =>
          knownTypes(sym)
        case t: TypeRef if t.takesTypeArgs =>
          // higherKind type
          val PolyType(typeParams, resultType) = t.etaExpand
          val clazz = TypeTree(internal.existentialType(typeParams, resultType))
          val typeArgs = typeParams.map(t => q"$TypeInfoT.noInfo")
          q"$TypeInfoT.apply(classOf[$clazz], ..$typeArgs)"
        case TypeRef(pre, sym, Nil) if sym.isModuleClass =>
          val clazz = if (sym.isJava) {
            TypeTree(internal.typeRef(pre, sym.companion, Nil))
          } else TypeTree(tpe)
          q"$TypeInfoT.javaTypeInfo(classOf[$clazz])"
        case AnnotatedType(_, underlying) =>
          mkTypeInfo(underlying)
        case ConstantType(value) =>
          // use the underlying tpe
          mkTypeInfo(value.tpe)
        case SingleType(_, _) =>
          val underlying = tpe.asInstanceOf[Types#SingleType].underlying
          mkTypeInfo(underlying.asInstanceOf[Type])
        case _ =>
          q"$typeInfoT[${TypeTree(forceResolve(tpe))}]"
      }
    }

    def forceResolve(tpe: Type): Type = {
      import scala.reflect.internal.Types

      val paramTypes = tpe.asInstanceOf[Types#Type].collect {
        case x: Types#TypeRef if (x.sym.isSkolem || (x.sym.isAbstract && !x.sym.isClass)) => x.sym.asInstanceOf[Symbol]
      }
      if (paramTypes.nonEmpty)
        tpe.substituteTypes(
          paramTypes,
          paramTypes.map(_.typeSignature match {
            case TypeBounds(_, hi) => forceResolve(hi)
            case x                 => c.abort(c.enclosingPosition, s"Can't handle $tpe's $x")
          }))
      else tpe
    }

    def flattenApplyArgs(a: Apply): (Tree, List[List[Tree]]) = {
      def flatten(t: Tree, sym: Symbol): List[Apply] = {
        t match {
          case a @ Apply(fun, args) if a.symbol == sym =>
            a :: flatten(fun, sym)
          case _ => Nil
        }
      }
      val f = flatten(a, a.symbol).reverse
      (f.head.fun, f.map(_.args))
    }
  }

  class Convertor(consts: Set[Tree]) extends Transformer {
    import Helper._

    import scala.util.Try

    private[this] var paramScope: ScopedHashMap[Symbol, TermName] = _

    def convertOption(t: Tree): Option[Tree] = {
      if (consts.contains(t)) None
      else
        Try {
          transform(t)
        } toOption
    }

    def convert(t: Tree): Tree = {
      transform(t)
    }

    override def transform(t: Tree): Tree = {
      if (consts.contains(t)) {
        val constant = t match {
          case Apply(Select(qual, TermName("augmentString")), List(arg)) if qual.symbol == definitions.PredefModule =>
            arg
          case _ => t
        }
        transformConstant(constant)
      } else
        t match {
          case Function(vparams, body) =>
            val paramDecls = vparams.map { case v @ ValDef(_, name, tpt, _) =>
              val rhs = q"$ElementFactoryT.parameter(${name.encodedName.toString}, ${mkTypeInfo(tpt.tpe)})"
              ValDef(NoMods, name, TypeTree(ParameterElementTpe), rhs)
            }
            val paramRefs = paramDecls.map(v => Ident(v.name))
            atParamScope(vparams) {
              val lambda = q"$ElementFactoryT.lambda(${transform(body)}, $ListT.apply(..$paramRefs))"
              q"..$paramDecls; $lambda"
            }

          case Match(s @ (Ident(_) | Typed(Ident(_), _)), List(CaseDef(a @ Apply(_, args), EmptyTree, body)))
              if s.tpe =:= a.tpe && a.tpe.typeSymbol.fullName == s"scala.Tuple${args.size}" =>
            // "yield ..." will generate x$1: (T1, T2) @unchecked match { ... }
            val paramDecl = ValDef(NoMods, TermName(c.freshName("x")), TypeTree(), transform(s))
            val paramRef = Ident(paramDecl.name)
            val typeDecl = ValDef(NoMods, TermName(c.freshName("tpe")), TypeTree(), mkTypeInfo(s.tpe))
            val typeRef = Ident(typeDecl.name)
            atParamScope(Nil) {
              val bindDecls = args.zipWithIndex.map {
                case (b @ Bind(name: TermName, (Ident(_) | Typed(Ident(_), _))), idx) =>
                  paramScope.put(b.symbol, name)
                  val member = q"new $FieldDescT($typeRef, ${s"_${idx + 1}"}, $typeRef.typeParams($idx))"
                  val rhs = q"$ElementFactoryT.makeMemberAccess($paramRef, $member)"
                  ValDef(NoMods, name, TypeTree(), rhs)
              }
              q"..${paramDecl :: typeDecl :: bindDecls}; ${transform(body)}"
            }

          case Match(
                s @ (Ident(_) | Typed(Ident(_), _)),
                List(
                  CaseDef(a @ Apply(_, args), EmptyTree, Literal(Constant(true))),
                  CaseDef(Ident(termNames.WILDCARD), EmptyTree, Literal(Constant(false))))) if s.tpe <:< a.tpe =>
            // generated filter lambda
            constElemTrueT

          case If(cond, thenp, elsep) =>
            val test = transform(cond)
            val ifTrue = transform(thenp)
            val ifFalse = transform(elsep)
            q"$ElementFactoryT.condition($test, $ifTrue, $ifFalse, ${mkTypeInfo(t.tpe)})"

          case Block(Nil, expr) =>
            transform(expr)

          case Block(
                List(ClassDef(_, _, Nil, Template(List(base), _, body))),
                expr @ Apply(Select(New(Ident(name)), _), Nil))
              if base.tpe =:= definitions.AnyRefTpe && name.encodedName.toString.startsWith("$anon") =>
            // new { ... }
            atParamScope(Nil) {
              val withOutCtor = body.filter {
                case DefDef(_, termNames.CONSTRUCTOR, _, _, _, _) => false
                case _                                            => true
              }
              val typeDecl = ValDef(NoMods, TermName(c.freshName(s"tpe$name")), TypeTree(), mkTypeInfo(expr.tpe))
              val typeRef = Ident(typeDecl.name)
              val argDecls = withOutCtor.map {
                case v @ ValDef(_, name, _, rhs) =>
                  paramScope.put(v.symbol, name)
                  ValDef(NoMods, name, TypeTree(RelationElementTpe), transform(rhs))

                case d @ DefDef(_, name, Nil, Nil, _, rhs) =>
                  paramScope.put(d.symbol, name)
                  ValDef(NoMods, name, TypeTree(RelationElementTpe), transform(rhs))

                case x =>
                  throw new RelationalUnsupportedException(s"Unsupported tree: $x")
              }
              val argRefs = withOutCtor.collect { case d @ DefDef(_, name, Nil, Nil, _, rhs) =>
                Ident(name)
              }
              val members = withOutCtor.collect { case d @ DefDef(_, name, Nil, Nil, tpt, rhs) =>
                q"new $FieldDescT($typeRef, ${name.encodedName.toString}, ${mkTypeInfo(tpt.tpe)})"
              }
              val ctor = q"new $CtorDescT($typeRef)"
              val nex = q"$ElementFactoryT.makeNew($ctor, $ListT.apply(..$argRefs) ,$ListT.apply(..$members))"
              q"..${typeDecl :: argDecls}; $nex"
            }

          case Apply(Select(qual, name), List(arg)) if binaryOps.isDefinedAt(name.encodedName.toString) =>
            val encoded = name.encodedName.toString
            if (encoded == "contains") {
              transformContains(transform(arg), transform(qual))
            } else {
              val method = q"new $MethodDescT(${mkTypeInfo(qual.tpe)}, $encoded, ${mkTypeInfo(t.tpe)})"
              transformBinary(encoded, transform(qual), transform(arg), method)
            }

          case Apply(Select(qual, TermName("augmentString")), List(arg)) if qual.symbol == definitions.PredefModule =>
            transform(arg)

          case Apply(TypeApply(Select(qual, name), List(_)), List(arg))
              if t.tpe =:= definitions.BooleanTpe && name.encodedName.toString == "contains" =>
            transformContains(transform(arg), transform(qual))

          case a: Apply =>
            val (qualifier, argss) = flattenApplyArgs(a)
            val paramss = qualifier.symbol.asMethod.paramLists
            val argElems = argss match {
              case args :: Nil =>
                val lst = paramss.head
                val params = if (lst.size < args.size) lst.padTo(args.size, lst.last) else lst
                args.zip(params).map(t => transformArgument(t._2.asTerm.isByNameParam, t._1))
              case _ =>
                argss.zip(paramss).map { args: (List[Tree], List[Symbol]) =>
                  val lst = args._2
                  val params = if (lst.size < args._1.size) lst.padTo(args._1.size, lst.last) else lst
                  val argList = args._1.zip(params).map(t => transformArgument(t._2.asTerm.isByNameParam, t._1))
                  q"new $ExpressionListElementT($ListT.apply(..$argList), $TypeInfoT.ANY)"
                }
            }
            qualifier match {
              case Select(New(tpt), _) => // do we want to support this?
                val ctor = q"new $CtorDescT(${mkTypeInfo(tpt.tpe)})"
                q"$ElementFactoryT.makeNew($ctor, $ListT.apply(..$argElems) ,$ListT.apply())"

              case Select(qual, name) =>
                val encodedName = name.encodedName.toString
                checkMethod(qualifier.symbol, encodedName)
                val inst = if (qual.symbol != null && qual.symbol.isModule) q"null" else transform(qual)
                val meta = getMemberMetadata(qualifier.symbol, qual.tpe)
                val method =
                  q"new $MethodDescT(${mkTypeInfo(qual.tpe)}, $encodedName, ${mkTypeInfo(t.tpe)}, metadata = $meta)"
                q"$ElementFactoryT.call($inst, $method, $ListT.apply(..$argElems))"

              case TypeApply(Select(qual, name), targs) =>
                val encodedName = name.encodedName.toString
                checkMethod(qualifier.symbol, encodedName)
                val inst = if (qual.symbol != null && qual.symbol.isModule) q"null" else transform(qual)
                val meta = getMemberMetadata(qualifier.symbol, qual.tpe)
                val method =
                  q"new $MethodDescT(${mkTypeInfo(qual.tpe)}, $encodedName, ${mkTypeInfo(t.tpe)}, $ListT.apply(..${targs
                      .map(ta => mkTypeInfo(ta.tpe))}), metadata = $meta)"
                q"$ElementFactoryT.call($inst, $method, $ListT.apply(..$argElems))"
            }

          case i: Ident if paramScope.contains(i.symbol) =>
            Ident(paramScope.getOrElse(i.symbol, ???))

          case s: Select if paramScope.contains(s.symbol) =>
            Ident(paramScope.getOrElse(s.symbol, ???))

          case Select(qual, name) =>
            val inst = transform(qual)
            if (canTranslateToMethodElement(t.symbol)) {
              val encodedName = name.encodedName.toString
              checkMethod(t.symbol, encodedName)
              val method = q"new $MethodDescT(${mkTypeInfo(qual.tpe)}, $encodedName, ${mkTypeInfo(t.tpe)})"
              q"$ElementFactoryT.call($inst, $method, $ListT.apply())"
            } else if (name.encodedName.toString == "unary_$bang" && t.tpe =:= definitions.BooleanTpe) {
              q"$ElementFactoryT.equal($inst, $constElemFalseT)"
            } else {
              val meta = getMemberMetadata(t.symbol, qual.tpe)
              val member =
                q"new $FieldDescT(${mkTypeInfo(qual.tpe)}, ${name.encodedName.toString}, ${mkTypeInfo(t.tpe)}, $meta)"
              q"$ElementFactoryT.makeMemberAccess($inst, $member)"
            }

          case TypeApply(Select(qual, name), targ :: Nil) =>
            val stringName = name.encodedName.toString
            if (stringName == "asInstanceOf") {
              // .asInstanceOf[...]
              if (qual.tpe =:= t.tpe) transform(qual)
              else {
                q"$ElementFactoryT.convert(${transform(qual)}, ${mkTypeInfo(t.tpe)})"
              }
            } else if (stringName == "isInstanceOf") {
              // .isInstanceOf[...]
              if (qual.tpe <:< targ.tpe) constElemTrueT
              else {
                q"$ElementFactoryT.typeIs(${transform(qual)}, ${mkTypeInfo(targ.tpe)})"
              }
            } else throw new RelationalUnsupportedException(s"Unsupported tree: $t")

          case Typed(expr, tpt) if expr.tpe =:= tpt.tpe =>
            transform(expr)

          case _ =>
            throw new RelationalUnsupportedException(s"Unsupported tree: $t")
        }
    }

    private def getMemberMetadata(memSym: Symbol, qualTp: Type): Tree = {
      def bytecodeMethodDescriptor: String = {
        val g = c.universe.asInstanceOf[scala.tools.nsc.Global]
        // In principle we would use
        //   g.enteringJVM(g.genBCode.bTypes.methodBTypeFromSymbol(memSym.asInstanceOf[g.Symbol]).descriptor)
        // but phase travelling (that much) to the future causes issues, for example:
        //  - types added to a symbol's type history are permanent, so if an InfoTransform (incorrectly)
        //    depends on some changes by a tree transform, the stored type is wrong.
        //  - `setInfoAndEnter` for synthetics does `owner.info.decls.enter`, which modifies the current
        //    `owner.info`, but not future types that might already exist.
        // So instead of using phase travel, we apply the relevant type transformations manually to obtain
        // the backend type.
        // Using `genBCode.bTypes.methodBTypeFromSymbol` also causes problems because it creates and caches
        // BTypes from referenced symbols, so it needs to run at the backend phase.

        def backendTp(tp: g.Type): g.Type =
          g.uncurry.uncurry(g.postErasure.elimErasedValueType(g.erasure.scalaErasure(tp)))
        val tp = backendTp(memSym.asInstanceOf[g.Symbol].tpe)

        def desc(tp: g.Type, b: StringBuilder): Unit = tp match {
          case g.MethodType(ps, res) =>
            b.append('(')
            ps.foreach(p => desc(p.info, b))
            b.append(')')
            desc(res, b)
          case g.TypeRef(_, g.definitions.ArrayClass, List(arg)) =>
            b.append('[')
            desc(arg, b)
          case _ =>
            g.genBCode.bTypes.coreBTypes.primitiveTypeToBType.get(tp.typeSymbol) match {
              case Some(bt) => b.append(bt.descriptor)
              case _ =>
                b.append('L').append(tp.typeSymbol.javaBinaryName).append(';')
            }
        }

        val b = new StringBuilder()
        desc(tp, b)
        b.toString
      }

      val isOverloadedCollectionsMethod =
        isOptionOrSeqOps(memSym) &&
          qualTp.member(memSym.name).alternatives.lengthCompare(1) > 0 &&
          c.universe.isInstanceOf[scala.tools.nsc.Global]
      val args = {
        val overload =
          if (isOverloadedCollectionsMethod)
            List(q"(${AbstractLambdaReifier.OverloadSignature}, $bytecodeMethodDescriptor)")
          else Nil
        if (memSym.annotations.exists(_.tree.tpe =:= ReifiedTpe))
          q"(${AbstractLambdaReifier.ReifiedTarget}, ${Select(Ident(memSym.owner.companion), TermName("" + memSym.name + "$reified"))})" :: overload
        else
          overload
      }
      if (args.nonEmpty) q"$MapT.apply(..$args)"
      else q"$MapT.empty"
    }

    private def transformArgument(isByName: Boolean, t: Tree): Tree = {
      if (!isByName) transform(t)
      else
        t match {
          case Function(Nil, _) => transform(t)
          case _                => transform(Function(Nil, t))
        }
    }

    private def atParamScope(params: List[ValDef])(f: => Tree): Tree = {
      val saveParamScope = paramScope
      paramScope = new ScopedHashMap[Symbol, TermName](paramScope)
      params.foreach { p =>
        paramScope.put(p.symbol, p.name)
      }
      val result = f
      paramScope = saveParamScope
      result
    }
  }

  /**
   * Nominate the constant candidates in the Tree
   */
  class ConstantNominator extends Traverser {
    import Helper._

    private[this] val candidates = new mutable.HashSet[Tree]
    private[this] var cannotBeConst: Boolean = _
    private[this] var symbolScope: ScopedHashMap[Symbol, Tree] = new ScopedHashMap[Symbol, Tree](null)
    private[this] var callScope: Symbol = _
    private[this] var instanceOfCall: Tree = EmptyTree

    def nominate(t: Tree): Set[Tree] = {
      traverse(t)
      candidates.toSet
    }

    override def traverse(tree: Tree): Unit = {
      val saveCannotBeConst = cannotBeConst
      cannotBeConst = false

      tree match {
        case Function(vparams, body)
            if (callScope eq null) ||
              canTranslateToMethodElement(callScope) ||
              (!candidates.contains(instanceOfCall) && isOptionOrSeqOps(callScope)) =>
          atOwner(tree.symbol) {
            atSymbolScope(vparams) {
              traverse(body)
            }
          }
          cannotBeConst = true

        case Function(vparams, body) =>
          if (hasSymbolRef(tree)) {
            atOwner(tree.symbol) {
              atSymbolScope(vparams) {
                traverse(body)
              }
            }
          }

        case Block(Nil, expr)
            if (callScope eq null) || canTranslateToMethodElement(callScope) || (!candidates.contains(
              instanceOfCall) && isOptionOrSeqOps(callScope)) =>
          traverse(expr)

        case Block(stats, expr) =>
          // if block has no param reference it could be a constant.
          // we do not need traverse into it
          if (hasSymbolRef(tree)) {
            atSymbolScope(Nil) {
              traverseTrees(stats)
              traverse(expr)
            }
          }

        case ClassDef(_, name, Nil, Template(List(base), _, body))
            if base.tpe =:= definitions.AnyRefTpe && name.encodedName.toString.startsWith("$anon") =>
          atOwner(tree.symbol) {
            symbolScope.put(tree.symbol, tree)
            traverseTrees(body)
          }

        case _: ClassDef =>
          symbolScope.put(tree.symbol, tree)
          cannotBeConst = true

        case _: ModuleDef =>
          symbolScope.put(tree.symbol, tree)
          cannotBeConst = true

        case ValDef(_, _, _, rhs) =>
          atOwner(tree.symbol) {
            symbolScope.put(tree.symbol, tree)
            traverse(rhs)
          }
          cannotBeConst = true

        case DefDef(_, _, _, vparamss, _, rhs) =>
          atOwner(tree.symbol) {
            symbolScope.put(tree.symbol, tree)
            atSymbolScope(vparamss.flatMap(identity)) {
              traverse(rhs)
            }
          }
          cannotBeConst = true

        case _: TypeDef =>
          symbolScope.put(tree.symbol, tree)
          cannotBeConst = true

        case _: Import =>
          cannotBeConst = true

        case TypeApply(fun, args) =>
          atCallScope(fun.symbol)(fun) {
            traverse(fun)
          }
          cannotBeConst |= args.exists(hasSymbolRef)
          if (cannotBeConst)
            removeCandidate(fun)

        case Select(qual, _) =>
          atCallScope(tree.symbol)(qual) {
            traverse(qual)
          }
          cannotBeConst |= canTranslateToMethodElement(tree.symbol)

        case a: Apply =>
          val (qual, argss) = flattenApplyArgs(a)
          atCallScope(a.fun.symbol)(qual) {
            traverse(qual)
            traverseParamss(argss)
          }
          if (argss.flatten.exists(!candidates.contains(_)))
            removeCandidate(qual)

        case CaseDef(pat, EmptyTree, body) =>
          val binds = pat.filter(t =>
            t match {
              case _: Bind => true
              case _       => false
            })
          atSymbolScope(binds) {
            traverse(body)
          }
          cannotBeConst = true

        case _: CaseDef =>
          cannotBeConst = true

        case Bind(name, body) =>
        case Ident(name) =>
          cannotBeConst = symbolScope.contains(tree.symbol) || name == termNames.WILDCARD

        case _: This =>
          cannotBeConst = symbolScope.contains(tree.symbol)

        case New(tpt) =>
          cannotBeConst = hasSymbolRef(tpt)

        case Typed(expr, tpt) =>
          traverse(expr)
          cannotBeConst |= hasSymbolRef(tpt)

        case _: Annotated =>
          cannotBeConst = true

        case _: Throw =>
          cannotBeConst = true

        case _: UnApply =>
          cannotBeConst = true

        case Return(expr) =>
          traverse(expr)
          cannotBeConst = true

        case _: Super =>
          cannotBeConst = true

        case _ =>
          super.traverse(tree)
      }

      if (!cannotBeConst)
        candidates.add(tree)
      cannotBeConst |= saveCannotBeConst
    }

    override def atOwner(owner: Symbol)(f: => Unit): Unit = {
      val saveCallScope = callScope
      callScope = NoSymbol
      super.atOwner(owner)(f)
      callScope = saveCallScope
    }

    private def atCallScope(sym: Symbol)(qual: Tree)(f: => Unit): Unit = {
      val saveCallScope = callScope
      val saveInstanceOfCall = instanceOfCall
      callScope = sym
      instanceOfCall = qual
      f
      instanceOfCall = saveInstanceOfCall
      callScope = saveCallScope
    }

    private def atSymbolScope(params: List[Tree])(f: => Unit): Unit = {
      val saveSymbolScope = symbolScope
      symbolScope = new ScopedHashMap[Symbol, Tree](symbolScope)
      params.foreach { p =>
        symbolScope.put(p.symbol, p)
      }
      f
      symbolScope = saveSymbolScope
    }

    private def removeCandidate(t: Tree): Unit = {
      if (candidates.remove(t)) t match {
        case TypeApply(fun, _) =>
          removeCandidate(fun)
        case Select(nex: New, termNames.CONSTRUCTOR) =>
          candidates.remove(nex)
        case Select(qual, _) if qual.symbol != null =>
          if (qual.symbol.isModule && qual.symbol.isJava)
            qual.foreach(candidates.remove(_))
        case _ =>
      }
    }

    private def hasSymbolRef(t: Tree): Boolean = {
      symbolScope != null && t.exists(_ match {
        case x: Ident =>
          symbolScope.contains(x.symbol)
        case x: This =>
          symbolScope.contains(x.symbol)
        case x: TypeTree =>
          symbolScope.exists(t => {
            val sym = t._1
            (sym.isType || sym.isModule) && x.tpe.contains(sym)
          })
        case _ => false
      })
    }
  }

  object UndoMacroTransformer extends Transformer {
    def undoMacro(t: Tree): Tree = {
      transform(t)
    }

    /**
     * we need to undo root macro then its subtrees' macro.
     */
    override def transform(t: Tree): Tree = {
      super.transform(undoOneMacro(t))
    }

    private def undoOneMacro(t: Tree): Tree = {
      val attachment = internal.attachments(t).get[StdAttachments#MacroExpansionAttachment]
      attachment
        .map(x => {
          val tree = x.expandee.asInstanceOf[Tree]
          // we reserve the tpe from macro expansion, thus white box macro (e.g. average)
          // would have a correct tpe in expandee
          internal.setType(tree, t.tpe)
        })
        .getOrElse(t)
    }
  }
}

class BasicLambdaReifier[C <: Context](val c: C) extends AbstractLambdaReifier
