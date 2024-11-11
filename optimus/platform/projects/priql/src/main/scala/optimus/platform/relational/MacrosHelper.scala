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

import optimus.platform._
import optimus.platform._
import optimus.platform.internal.MacroBase
import optimus.platform.storable.Entity
import optimus.platform.relational.tree.TypeInfo

import scala.collection.mutable
import scala.tools.nsc.typechecker.StdAttachments
import scala.reflect.macros.blackbox.Context

object MacrosHelper {

  /**
   * this method is used to get a seq of node function trees (q: Query[T1]) => U1; (q: Query[T2)) => U2 ... and a seq of
   * aggregate types here T1, T2... composites GroupValueType, U1, U2... composites AggregateValueType
   */
  def getNodeFuncTreesAndAggregateTypes(
      c: Context)(groupValueType: c.Type, aggregaValueType: c.Type): (List[c.Tree], List[c.Tree]) = {
    import c.universe._

    val util = new AggregateByImplicitUtils[c.type](c)
    val impls = (groupValueType, aggregaValueType) match {
      case (groupValue: TypeRef, aggregate: TypeRef) =>
        val implicitAggregatorNodeFuc = util.getNodeFuncTree(groupValue, aggregate)
        List(implicitAggregatorNodeFuc)
      case (RefinedType(parents1, _), RefinedType(parents2, _)) =>
        if (parents1.length != parents2.length)
          c.error(
            c.macroApplication.pos,
            "the number of trait or class combining GroupValueType and AggregateType should be the same")
        parents1 zip parents2 map { case (groupValue, aggregate) =>
          util.getNodeFuncTree(groupValue, aggregate)
        }
    }
    (impls, util.getAggregateTypes)
  }

  /**
   * this method is used to construct a function tree (q: Query[groupValue]) => groupValue based on customLambda
   */
  def constructNodeFuncTree(c: Context)(groupValue: c.Type, customLambda: c.Tree): c.Tree =
    new AggregateByImplicitUtils[c.type](c).constructCustomNodeFuncTree(groupValue, customLambda)
}

/**
 * since we don't want to expose aggregateTypes: ListBuffer[c.Tree] to client so separated it to be a private class and
 * client could only interact with AggregateByImplicitUtils object
 */
class AggregateByImplicitUtils[C <: Context](val c: C) extends MacroBase {
  import c.universe._

  private val predefMethods = Set(
    "hashCode",
    "toString",
    "productIterator",
    "productElements",
    "productElementNames",
    "getClass",
    "productPrefix",
    "productArity",
    "isInstanceOf",
    "asInstanceOf") ++ List(classOf[scala.Any], classOf[Entity], classOf[Object])
    .flatMap(_.getMethods)
    .map(_.getName)

  private val ValdefSuffix = "0"

  val queryType = typeOf[Query[_]]
  val nodeFuncType = typeOf[NodeFunction1[_, _]]
  val numericType = typeOf[Numeric[_]]
  val parameterName = c.freshName("q")

  private val aggregateTypes = mutable.ListBuffer[c.Tree]()

  def getAggregateTypes: List[Tree] = aggregateTypes.toList

  /**
   * first infer whether there is a predefined implicit node function tree (q: Query[groupValue]) => aggregate, if there
   * isn't such a tree, we will see whether groupValue is equal to aggregate:
   *
   * Yes: we will generate one node function tree according to default aggregate rules on different types
   *
   * No: throw exception
   *
   * if there is, we will return this node function tree.
   */
  def getNodeFuncTree(groupValue: Type, aggregate: Type): Tree = {
    aggregateTypes += new TypeInfo.SummonImpl[c.type](c).summon(aggregate)

    inferImplicitNodeFuncValueTree(groupValue, aggregate).getOrElse {
      if (groupValue == aggregate) constructDefaultNodeFuncValueTree(groupValue)
      else {
        c.error(
          c.macroApplication.pos,
          s"groupValue type ${groupValue} is not equal to aggregate type ${aggregate} and macro could not find implicit NodeFunction value."
        )
        EmptyTree
      }
    }
  }

  /**
   * generate a node function tree (q: Query[groupValue]) => groupValue based on customLambda and default aggregate
   * rules
   */
  def constructCustomNodeFuncTree(groupValue: Type, customLambda: Tree): Tree = {
    def untypecheck(t: Tree, vparam: ValDef): Tree = {
      val localResetTree = c.untypecheck(t)
      // in 2.11 macro there is not resetAllAttrs method but only untypecheck which is to reset local attrs (make these
      // attrs' symbol and tpe to NoSymbol and null) but only to make global attrs' tpe null. so we need to make global
      // attrs' symbol NoSymbol manually
      val symbols = Set(vparam.symbol, vparam.symbol.owner)
      new ResetOuterAttributeSymbolTraverser(symbols).traverse(localResetTree)
      localResetTree
    }

    /**
     * pick up all DefDefs in new { ... }, these DefDefs are all custom aggregate rules to fields in groupValue
     */
    class DefDefPicker(vparam: ValDef) extends Traverser {
      private val defMap = new mutable.ListBuffer[(String, Tree)]()

      def getDefMap = defMap.toMap

      override def traverse(tree: Tree) = tree match {
        case DefDef(mods, name, _, _, tpt, rhs) => {
          val defName = name.decodedName.toString + ValdefSuffix
          val originalRhs = new WholeTreeUndoMacroExpansionTransformer[c.type](c).transform(rhs)
          val newRhs = untypecheck(originalRhs, vparam)
          defMap += ((defName, newRhs))
        }
        case _ => super.traverse(tree)
      }
    }

    /**
     * replace default aggregate rule with custom rule in defMap if there is one
     */
    class ReplaceValDefTransformer(val defMap: Map[String, Tree]) extends Transformer {
      override def transform(tree: Tree) = tree match {
        case ValDef(mods, name, tpt, rhs) if defMap.contains(name.decodedName.toString) =>
          treeCopy.ValDef(tree, mods, name, tpt, defMap.get(name.decodedName.toString).get)
        case _ => super.transform(tree)
      }
    }

    /**
     * we may have other statements in custom function, insert them into new node function E.g.
     *
     * "val minOuter = q.min(_.name)" in (q: Query[CompositeCoordinates]) => { val minOuter = q.min(_.name); new {def
     * name = minOuter + getSuffix} }
     */
    class InsertStatsIntoTree(val stats: List[Tree], vparam: ValDef) extends Transformer {
      override def transform(tree: Tree) = tree match {
        // the first tree to match this pattern is what we want since the input tree is DefaultNodeFunction tree that we know what it looks like
        case Block(originalStats: List[Tree], expr @ Block(List(ClassDef(_, name, _, _)), Apply(_, _)))
            if (name.decodedName.toString.startsWith("anon")) =>
          val untypeCheckTrees =
            stats.map(t => untypecheck(new WholeTreeUndoMacroExpansionTransformer[c.type](c).transform(t), vparam))
          val treet = treeCopy.Block(tree, untypeCheckTrees ++ originalStats, expr)
          treet
        case _ => super.transform(tree)
      }
    }

    /**
     * since the generated DefaultNodeFunction tree's parameter is q: Query which may be conflict with some field in
     * user's custom nodefunction, we adopt parameter in user's custom nodefunction tree and use this one to replace q:
     * Query
     */
    class NodeFuncParamReplacer(replacedName: String, replacer: String) extends Transformer {
      override def transform(tree: Tree): Tree = {
        tree match {
          case Ident(name) if (name.decodedName.toString == replacedName) =>
            treeCopy.Ident(tree, TermName(replacer)) // some q in q.sum....
          case ValDef(m, name, tpt, rhs) if (name.decodedName.toString == replacedName) =>
            treeCopy.ValDef(tree, m, TermName(replacer), tpt, rhs) // q in (q: Query) => ...
          case _ => super.transform(tree)
        }
      }
    }

    def handleClassDef(classDef: Tree, vparam: ValDef, templateMiddleTree: Tree): Tree = {
      classDef match {
        case ClassDef(_, name, tparams, impl @ Template(parents, _, _))
            // which could check whether this anonymous class just extends AnyRef
            if name.decodedName.toString == "$anon" && parents.length == 1 =>
          val defDefPicker = new DefDefPicker(vparam)
          defDefPicker.traverse(impl)
          val defMap = defDefPicker.getDefMap
          val replacedTree = new ReplaceValDefTransformer(defMap).transform(templateMiddleTree)
          new NodeFuncParamReplacer(parameterName, vparam.name.decodedName.toString).transform(replacedTree)
        case _ =>
          c.error(
            c.macroApplication.pos,
            s"customAggregate is called incorrectly, the right format should be customAggregate { (q: Query[Type]) => ...; new { def fieldOneInType = q.aggregateOperatorOne(_.fieldOneInType);  def fieldTwoInType = q.aggregateOperatorTwo(_.fieldTwoInType); ... } } "
          )
          EmptyTree
      }
    }

    val middleTree = constructDefaultNodeFuncValueTree(groupValue)
    val finalTree = customLambda match {
      case Function(vparam, Block(stats, Apply(_, _))) if (vparam.size == 1 && stats.size == 1) =>
        handleClassDef(stats.head, vparam.head, middleTree)
      case Function(vparam, Block(outerDefs, Block(stats, Apply(_, _)))) if (vparam.size == 1 && stats.size == 1) =>
        val t = handleClassDef(stats.head, vparam.head, middleTree)
        new InsertStatsIntoTree(outerDefs, vparam.head).transform(t)
      case _ =>
        c.error(
          c.macroApplication.pos,
          s"customAggregate is called incorrectly, the right format should be customAggregate { (q: Query[Type]) => ...; new { def fieldOneInType = q.aggregateOperator(_.fieldOneInType) ... } } "
        )
        EmptyTree
    }
    finalTree
  }

  /**
   * generate node function tree based on default aggregate rules:
   *
   * String: call StringAggregator
   *
   * Option[T]: call T's OptionAggregator
   *
   * Others: see whether there is an implicit Numeric to this type, do aggregation on Numeric
   */
  private def constructDefaultNodeFuncValueTree(groupValue: Type): Tree = {
    val members = groupValue.members.filter(mem => {
      if (mem.isMethod) {
        val name = mem.asMethod.name.toString
        val paramss = mem.asMethod.paramLists
        !mem.isStatic && mem.isPublic && paramss.isEmpty && (!name.contains("$") && !name
          .contains("copy$default") && !predefMethods.exists(s => name.contains(s)))
      } else false
    })
    if (members.exists(_.asMethod.annotations.nonEmpty))
      c.error(
        c.macroApplication.pos,
        "there is field in groupValue having annotation during creating defdef in new groupValue { ... } at macro time")
    val nameWithTypes = members.map(m => (m.asMethod.name, m.asMethod.returnType))
    val valDefs = DefaultRuleDefConstructor.constructValDefs(nameWithTypes, groupValue)
    val defDefs = DefaultRuleDefConstructor.constructDefDefs(nameWithTypes, groupValue)
    insertDefDefsAndValDefsIntoAsNodeTree(defDefs, valDefs, groupValue)
  }

  object DefaultRuleDefConstructor {
    def constructDefDefs(nameWithTypes: Iterable[(Name, Type)], shapeType: Type): Iterable[DefDef] = {

      def constructDefDef(name: Name, ntype: Type, shapeType: Type): DefDef = {
        val defName = name.decodedName.toString
        DefDef(Modifiers(), TermName(defName), Nil, Nil, TypeTree(ntype), Ident(TermName(defName + ValdefSuffix)))
      }

      val defDefs = nameWithTypes.map { case (fname, ftype) => constructDefDef(fname, ftype, shapeType) }
      defDefs
    }

    def constructValDefs(nameWithTypes: Iterable[(Name, Type)], shapeType: Type): Iterable[ValDef] = {
      var num = -1
      nameWithTypes.map { case (fname, ftype) =>
        num += 1
        constructValDef(fname, ftype, shapeType, num)
      }
    }

    /**
     * construct a ValDef name = q."aggregate" the aggregate here is inferred by ntype's default aggregate rule
     */
    private def constructValDef(name: Name, ntype: Type, shapeType: Type, num: Int, optionNum: Int = 0): ValDef = {
      ntype match {
        case TypeRef(_, _, List(fftype)) if (ntype <:< typeOf[Option[_]]) =>
          constructValDef(name, fftype, shapeType, num, optionNum + 1)
        case strType if (strType =:= typeOf[String]) =>
          constructStrAggregatorValDef(name, ntype, shapeType, num, optionNum)
        case tp
            if (tp =:= c.typeOf[Double] || tp =:= c.typeOf[Float] || tp =:= c.typeOf[Int] || tp =:= c.typeOf[Long]) =>
          constructSumValDef(name, ntype, shapeType, num, optionNum)
        case _ =>
          val numericAppliedType = appliedType(numericType, List(ntype))
          if (c.inferImplicitValue(numericAppliedType) == EmptyTree)
            c.error(c.macroApplication.pos, s"field name ${name} doesn't have implicit value for Numeric[${ntype}]")
          constructSumValDef(name, ntype, shapeType, num, optionNum)
      }
    }
    // string default rule
    private def constructStrAggregatorValDef(
        fname: Name,
        ftype: Type,
        shapeType: Type,
        num: Int,
        optionNum: Int = 0): ValDef = {
      val (defType, secondParamter) =
        if (optionNum == 0)
          (
            TypeTree(ftype),
            Select(Ident(TermName("Aggregator")), TermName("StringAggregator"))
          ) // Aggregator.StringAggregator
        else
          (
            TypeTree(constructOptionType(ftype, optionNum)),
            TypeApply(
              Select(Ident(TermName("Aggregator")), TermName("Option")),
              List(constructAppliedTypeTree(optionNum - 1))
            )
          ) // Aggregator.Option[...Option[String]]
      val valDef = ValDef(Modifiers(), TermName("x$" + num), TypeTree(shapeType), EmptyTree) // x$1: shapeType
      val function = Function(
        List(valDef),
        Select(Ident(TermName("x$" + num)), TermName(fname.decodedName.toString))
      ) // x$1: shapeType => x$1.fname
      val func = Apply(
        Select(Ident(TermName(parameterName)), TermName("aggregate")),
        List(function)
      ) // q.aggregate(x$1: shapeType => x$1.fname)
      val rhs = Apply(
        func,
        List(secondParamter)
      ) // q.aggregate(x$1: shapeType => x$1.fname)(Aggregator.StringAggregator || Aggregator.Option[...Option[String]])
      ValDef(
        Modifiers(),
        TermName(fname.decodedName.toString + ValdefSuffix),
        defType,
        rhs
      ) // val fname: Option[...Option[String]] || String = q.aggregate(((x$1: shapeType) => x$1.fname))(Aggregator.StringAggregator)
    }
    // type which has implicit Numeric object default rule or Int, Long, Double, Float
    private def constructSumValDef(fname: Name, ftype: Type, shapeType: Type, num: Int, optionNum: Int): ValDef = {
      val valDef = ValDef(Modifiers(), TermName("x$" + num), TypeTree(shapeType), EmptyTree) // x$1: shapeType
      val function = Function(
        List(valDef),
        Select(Ident(TermName("x$" + num)), TermName(fname.decodedName.toString))
      ) // x$1: shapeType => x$1.fname
      val rhs = Apply(
        Select(Ident(TermName(parameterName)), TermName("sum")),
        List(function)
      ) // q.aggregate(x$1: shapeType => x$1.fname)
      ValDef(
        Modifiers(),
        TermName(fname.decodedName.toString + ValdefSuffix),
        TypeTree(constructOptionType(ftype, optionNum)),
        rhs
      ) // val fname: Option[...Option[String]] || String = q.aggregate(((x$1: shapeType) => x$1.fname))(Aggregator.StringAggregator)
    }

    private def constructAppliedTypeTree(optionNum: Int): Tree = {
      if (optionNum == 0) Ident(TypeName("String"))
      else AppliedTypeTree(Ident(TypeName("Option")), List(constructAppliedTypeTree(optionNum - 1)))
    }

    private def constructOptionType(innerType: Type, optionNum: Int): Type = {
      if (optionNum == 0) innerType
      else appliedType(definitions.OptionClass, List(constructOptionType(innerType, optionNum - 1)))
    }
  }

  def insertDefDefsAndValDefsIntoAsNodeTree(
      defDefs: Iterable[DefDef],
      valDefs: Iterable[ValDef],
      groupValue: Type): Tree = {
    val anonName = c.freshName("anon")
    val param = ValDef(
      Modifiers(),
      TermName(parameterName),
      TypeTree(appliedType(queryType, List(groupValue))),
      EmptyTree
    ) // q: Query[groupValue]
    val newAnonClass = Apply(Select(New(Ident(TypeName(anonName))), termNames.CONSTRUCTOR), Nil) // new $anon
    val superInit = DefDef(
      Modifiers(),
      termNames.CONSTRUCTOR,
      Nil,
      List(Nil),
      TypeTree(NoType),
      Block(
        List(Apply(Select(Super(This(TypeName("")), TypeName("")), termNames.CONSTRUCTOR), Nil)),
        Literal(Constant("()"))
      )
    ) // def <init>() = {super.<init>();()}
    val classBody = List(superInit) ++ defDefs
    val classDef = ClassDef(
      Modifiers(),
      TypeName(anonName),
      Nil,
      Template(
        List(Ident(groupValue.typeSymbol)),
        ValDef(Modifiers(), TermName("_"), TypeTree(NoType), EmptyTree),
        classBody)
    )
    Apply(
      Select(Ident(TermName("asNode")), TermName("apply")),
      List(Function(List(param), Block(valDefs.toList, Block(List(classDef), newAnonClass)))))

  }

  def inferImplicitNodeFuncValueTree(groupValue: Type, aggregate: Type): Option[Tree] = {
    val left = appliedType(queryType, List(groupValue)) // Query[GroupValueType]
    val right = aggregate
    val implicitNodeFuncType =
      appliedType(nodeFuncType, List(left, right)) // NodeFunction1[Query[GroupValueType], AggregateType]
    val implicitAggregatorNodeFunc = c.inferImplicitValue(implicitNodeFuncType)
    if (implicitAggregatorNodeFunc == EmptyTree) None else Some(implicitAggregatorNodeFunc)
  }

  /**
   * reset the tree's symbol to NoSymbol if the tree is Ident && it comes from outer scope definition and we only
   * reference it here e.g. t.map(f => f.position).sum + outSum here t and outSum comes from outer scope but not f
   */
  class ResetOuterAttributeSymbolTraverser(val rootResetSymbols: Set[Symbol]) extends Traverser {
    private val allResetSymbols = new mutable.HashSet[Symbol]().++=(rootResetSymbols)
    override def traverse(tree: c.Tree) = tree match {
      case i: Ident if (allResetSymbols.contains(i.symbol) || allResetSymbols.contains(i.symbol.owner)) =>
        allResetSymbols += (i.symbol)
        allResetSymbols += (i.symbol.owner)
        internal.setSymbol(tree, NoSymbol)
      case _ => super.traverse(tree)
    }
  }

}

/**
 * undo all macros applied to this tree and its sub trees
 */
class WholeTreeUndoMacroExpansionTransformer[C <: Context](val c: C) {

  import c.universe._

  def transform(tree: Tree): Tree = UndoMacroTransformerImpl.transform(tree)

  object UndoMacroTransformerImpl extends Transformer {
    def undoOneMacro(t: Tree): Tree = {
      internal
        .attachments(t)
        .get[StdAttachments#MacroExpansionAttachment]
        .map(_.expandee.asInstanceOf[Tree])
        .getOrElse(t)
    }

    /**
     * we need to undo root macro then its subtrees'. If the order is reversed, that is after undoing subtree then root,
     * all subtrees's macro will come back again
     */
    override def transform(tree: Tree): Tree = {
      val originalTree = undoOneMacro(tree)
      super.transform(originalTree)
    }
  }

}
