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
package optimus.platform.relational.pivot

import optimus.platform._
import optimus.platform._
import optimus.platform.relational.internal.OptimusCoreAPI._
import optimus.platform.relational.tree.TypeInfo

import scala.collection.mutable.ListBuffer
import scala.reflect.macros.whitebox.Context
import scala.tools.nsc.typechecker.StdAttachments

object PivotTableMacros {

  val excludeMethods = List(
    "$asInstanceOf",
    "$isInstanceOf",
    "##",
    "finalize",
    "wait",
    "notifyAll",
    "notify",
    "toString",
    "clone",
    "hashCode",
    "getClass",
    "asInstanceOf",
    "isInstanceOf")

  def pivotOn[SrcType: c.WeakTypeTag, U: c.WeakTypeTag, RemainingType: c.WeakTypeTag](c: Context {
    type PrefixType = Query[SrcType]
  })(pivotOnLambda: c.Expr[SrcType => U]): c.Expr[Any] = {
    import c.universe._

    val fieldNames = new ListBuffer[c.Expr[String]]()
    val fieldNamesSet = new ListBuffer[String]()

    def fetchFieldName(tree: Select) = {
      fieldNamesSet += tree.name.decodedName.toString
      fieldNames += c.Expr[String](Literal(Constant(tree.name.decodedName.toString)))
    }

    // Get fields from pivotOnLambda.tree
    pivotOnLambda.tree match {
      case Function(_, body: Select) => {
        fetchFieldName(body)
      }
      case Function(_, body: Apply) => {
        body.args.foreach(f => {
          f match {
            case select: Select => fetchFieldName(select)
            case _ =>
              throw new IllegalArgumentException(
                "Do not support pivoting on multi-level call, e.g. pivotOn(t => t.area.log), only support one level call e.g. pivotOn(t: Trade => t.area), pivotOn(t => (t.area,t.book) )")
          }
        })
      }
      case _ =>
        throw new IllegalArgumentException(
          "Do not support pivoting on multi-level call, e.g. pivotOn(t => t.area.log), only support one level call e.g.pivotOn(t: Trade => t.area), pivotOn(t => (t.area,t.book) )")
    }

    val oldPivotTableTreeExpr =
      createPivotTableExprFromPivotOn[SrcType, RemainingType](c)(fieldNamesSet.toSet, fieldNames.toList)
    val newPivotTableTree = transformToNewPivotTableTreeWithLeftTypeClass(c)(oldPivotTableTreeExpr.tree)
    c.Expr[Any](newPivotTableTree)
  }

  def pivotOnTyped[SrcType: c.WeakTypeTag, PivotOnType >: SrcType: c.WeakTypeTag, RemainingType: c.WeakTypeTag](
      c: Context { type PrefixType = Query[SrcType] }): c.Expr[Any] = {
    import c.universe._

    val fieldNamesSet: List[String] = weakTypeOf[PivotOnType].members
      .collect {
        case d
            if d.isMethod && d.asMethod.paramLists.flatten.isEmpty && (d.asTerm.name != termNames.CONSTRUCTOR) && !excludeMethods
              .contains(d.asTerm.name.decodedName.toString) =>
          d.asMethod
      }
      .iterator
      .map(_.name.decodedName.toString)
      .toList
    // first should make sure that additional field names should be subset of field names in RemainingType, otherwise it means user uses this GroupOnType on groupOnTyped more than once
    val remainingTypeFieldNames: List[String] = weakTypeOf[RemainingType].decls
      .collect {
        case d
            if d.isMethod && d.asMethod.paramLists.flatten.isEmpty && (d.asTerm.name != termNames.CONSTRUCTOR) && d.asTerm.name.decodedName.toString != "map" =>
          d.asMethod
      }
      .iterator
      .map(_.name.decodedName.toString)
      .toList
    fieldNamesSet.foreach(f =>
      if (!remainingTypeFieldNames.contains(f))
        throw new IllegalArgumentException("you can not use any field which is not in RemainingType"))
    val fieldNames = fieldNamesSet.map(f => c.Expr[String](Literal(Constant(f))))

    val oldPivotTableTreeExpr =
      createPivotTableExprFromPivotOn[SrcType, RemainingType](c)(fieldNamesSet.toSet, fieldNames)
    val newPivotTableTree = transformToNewPivotTableTreeWithLeftTypeClass(c)(oldPivotTableTreeExpr.tree)
    c.Expr[Any](newPivotTableTree)
  }

  def addPivotField[
      SrcType: c.WeakTypeTag,
      U: c.WeakTypeTag,
      R <: DynamicObject: c.WeakTypeTag,
      LeftType: c.WeakTypeTag](c: Context { type PrefixType = PivotTable[LeftType, R, SrcType] })(
      lambda: c.Expr[LeftType => U]): c.Expr[Any] = {
    import c.universe._

    // Get fields from pivotOnLambda.tree
    val fieldNames = new ListBuffer[c.Expr[String]]()
    val fieldNamesSet = new ListBuffer[String]()

    lambda.tree match {
      case Function(_, body: Select) => {
        fieldNamesSet += body.name.decodedName.toString
        fieldNames += c.Expr[String](Literal(Constant(body.name.decodedName.toString)))
      }
      case _ =>
        throw new IllegalArgumentException(
          "Do not support addPivotField on multi-level call and multi-field call, e.g. addPivotField(t => t.area.log) and addPivotField(t => (t.area, t.portfolio)), only support one level call and one field call e.g. addPivotField(t: Trade => t.area)")
    }

    val oldPivotTableTreeExpr = createPivotTableExprFromAddPivotField(c)(fieldNamesSet.toSet, fieldNames.toList)
    val newPivotTableTree = transformToNewPivotTableTreeWithLeftTypeClass(c)(oldPivotTableTreeExpr.tree)
    c.Expr[Any](newPivotTableTree)
  }

  def addRowField[SrcType: c.WeakTypeTag, U: c.WeakTypeTag, R <: DynamicObject: c.WeakTypeTag, LeftType: c.WeakTypeTag](
      c: Context { type PrefixType = PivotTable[LeftType, R, SrcType] })(lambda: c.Expr[LeftType => U]): c.Expr[Any] = {
    import c.universe._

    val srcFieldNames: Set[String] = weakTypeOf[SrcType].decls
      .collect {
        case d if d.isMethod && d.asMethod.paramLists.flatten.isEmpty && (d.asTerm.name != termNames.CONSTRUCTOR) =>
          d.asMethod
      }
      .iterator
      .map(_.name.decodedName.toString)
      .toSet
    val preRowFieldMethods = weakTypeOf[R].decls
      .filter(s =>
        s.isMethod && s.asMethod.paramLists.flatten.isEmpty && srcFieldNames.contains(
          s.asMethod.name.decodedName.toString))
      .toSet
    val preRowFieldNames: Set[String] = preRowFieldMethods.map(f => f.asMethod.name.decodedName.toString)

    val addFieldNames = new ListBuffer[String]
    val addFieldNameExprs = new ListBuffer[c.Expr[String]]()

    lambda.tree match {
      case Function(_, body: Select) => {
        addFieldNameExprs += c.Expr[String](Literal(Constant(body.name.decodedName.toString)))
        addFieldNames += body.name.decodedName.toString
      }
      case _ =>
        throw new IllegalArgumentException(
          "not support addRowField on multi-level call and multi-field call, e.g. addRowField(t => t.area.log) and addRowField(t => (t.area, t.portfolio)), only support one level call and one field call, e.g. addRowField(t: Trade => t.area)")
    }

    val oldPivotTableTreeExpr =
      createPivotTableExprFromAddRowAndGroupOn(c)(preRowFieldNames, addFieldNames.toSet, addFieldNameExprs.toList)

    val midPivotTableTree = transformToNewPivotTableTreeWithLeftTypeClass(c)(oldPivotTableTreeExpr.tree)
    val newPivotTableTree = transformToNewPivotTableTreeWithPivotRowTypeClass(c)(midPivotTableTree)
    c.Expr[Any](newPivotTableTree)
  }

  def addDataField[SrcType: c.WeakTypeTag, R <: DynamicObject: c.WeakTypeTag, LeftType: c.WeakTypeTag](c: Context {
    type PrefixType = PivotTable[LeftType, R, SrcType]
  })(columnName: c.Expr[String], lambda: c.Expr[Query[SrcType] => Any]): c.Expr[Any] = {
    import c.universe._

    val body = reify {
      val lambda1 = Lambda1(None, Some(liftNode(lambda.splice)))
      val src = c.prefix.splice
      src.addDataFieldList(Seq(DataField(columnName.splice, lambda1)))
    }
    body
  }

  def groupOn[
      SrcType: c.WeakTypeTag,
      U: c.WeakTypeTag,
      RowType <: DynamicObject: c.WeakTypeTag,
      RemainingType: c.WeakTypeTag](c: Context { type PrefixType = PivotTable[RemainingType, RowType, SrcType] })(
      lambda: c.Expr[RemainingType => U]): c.Expr[Any] = {
    import c.universe._

    val srcFieldNames: Set[String] = weakTypeOf[SrcType].decls
      .collect {
        case d if d.isMethod && d.asMethod.paramLists.flatten.isEmpty && (d.asTerm.name != termNames.CONSTRUCTOR) =>
          d.asMethod
      }
      .iterator
      .map(_.name.decodedName.toString)
      .toSet
    val preRowFieldMethods = weakTypeOf[RowType].decls
      .filter(s =>
        s.isMethod && s.asMethod.paramLists.flatten.isEmpty && srcFieldNames.contains(
          s.asMethod.name.decodedName.toString))
      .toSet
    val preRowFieldNames: Set[String] = preRowFieldMethods.map(f => f.asMethod.name.decodedName.toString)

    val addFieldNames = new ListBuffer[String]
    val addFieldNameExprs = new ListBuffer[c.Expr[String]]()

    lambda.tree match {
      case Function(_, body: Apply) => {
        body.args.foreach(f => {
          f match {
            case select: Select =>
              addFieldNameExprs += c.Expr[String](Literal(Constant(select.name.decodedName.toString)))
              addFieldNames += select.name.decodedName.toString
            case _ =>
              throw new IllegalArgumentException(
                "not support group on multi-level call, e.g. groupOn(t => t.area.log), only support one level call e.g. groupOn(t: Trade => t.area)")
          }
        })
      }
      case Function(_, body: Select) => {
        addFieldNameExprs += c.Expr[String](Literal(Constant(body.name.decodedName.toString)))
        addFieldNames += body.name.decodedName.toString

      }
      case _ =>
        throw new IllegalArgumentException(
          "Do not support group on multi-level call, e.g. groupOn(t => t.area.log), only support one level call e.g. groupOn(t: Trade => t.area)")
    }

    val oldPivotTableTreeExpr =
      createPivotTableExprFromAddRowAndGroupOn(c)(preRowFieldNames, addFieldNames.toSet, addFieldNameExprs.toList)

    val midPivotTableTree = transformToNewPivotTableTreeWithLeftTypeClass(c)(oldPivotTableTreeExpr.tree)
    val newPivotTableTree = transformToNewPivotTableTreeWithPivotRowTypeClass(c)(midPivotTableTree)
    c.Expr[Any](newPivotTableTree)
  }

  def groupOnTyped[
      SrcType: c.WeakTypeTag,
      GroupOnType: c.WeakTypeTag,
      RowType <: DynamicObject: c.WeakTypeTag,
      RemainingType: c.WeakTypeTag](
      c: Context { type PrefixType = PivotTable[RemainingType, RowType, SrcType] }): c.Expr[Any] = {
    import c.universe._

    val srcFieldNames: Set[String] = weakTypeOf[SrcType].decls
      .collect {
        case d if d.isMethod && d.asMethod.paramLists.flatten.isEmpty && (d.asTerm.name != termNames.CONSTRUCTOR) =>
          d.asMethod
      }
      .iterator
      .map(_.name.decodedName.toString)
      .toSet
    val preRowFieldMethods = weakTypeOf[RowType].decls
      .filter(s =>
        s.isMethod && s.asMethod.paramLists.flatten.isEmpty && srcFieldNames.contains(
          s.asMethod.name.decodedName.toString))
      .toSet
    val preRowFieldNames: Set[String] = preRowFieldMethods.map(f => f.asMethod.name.decodedName.toString)

    val addFieldNames: List[String] = weakTypeOf[GroupOnType].members
      .collect {
        case d
            if d.isMethod && d.asMethod.paramLists.flatten.isEmpty && (d.asTerm.name != termNames.CONSTRUCTOR) && !excludeMethods
              .contains(d.asTerm.name.decodedName.toString) =>
          d.asMethod
      }
      .iterator
      .map(_.name.decodedName.toString)
      .toList
    // first should make sure that additional field names should be subset of field names in RemainingType, otherwise it means user uses this GroupOnType on groupOnTyped more than once
    val remainingTypeFieldNames: List[String] = weakTypeOf[RemainingType].decls
      .collect {
        case d
            if d.isMethod && d.asMethod.paramLists.flatten.isEmpty && (d.asTerm.name != termNames.CONSTRUCTOR) && d.asTerm.name.decodedName.toString != "map" =>
          d.asMethod
      }
      .iterator
      .map(_.name.decodedName.toString)
      .toList
    addFieldNames.foreach(f =>
      if (!remainingTypeFieldNames.contains(f))
        throw new IllegalArgumentException("you can not use any field which is not in RemainingType"))
    val addFieldNameExprs = addFieldNames.map(f => c.Expr[String](Literal(Constant(f))))

    val oldPivotTableTreeExpr =
      createPivotTableExprFromAddRowAndGroupOn(c)(preRowFieldNames, addFieldNames.toSet, addFieldNameExprs.toList)

    val midPivotTableTree = transformToNewPivotTableTreeWithLeftTypeClass(c)(oldPivotTableTreeExpr.tree)
    val newPivotTableTree = transformToNewPivotTableTreeWithPivotRowTypeClass(c)(midPivotTableTree)
    c.Expr[Any](newPivotTableTree)
  }

  private def createPivotTableExprFromAddRowAndGroupOn[
      SrcType: c.WeakTypeTag,
      RowType <: DynamicObject: c.WeakTypeTag,
      RemainingType: c.WeakTypeTag](c: Context { type PrefixType = PivotTable[RemainingType, RowType, SrcType] })(
      preRowFieldNames: Set[String],
      addRowFieldNames: Set[String],
      addRowFieldNameExprs: List[c.Expr[String]]): c.Expr[Any] = {
    import c.universe._

    val newFieldNameExprs: c.Expr[List[String]] = mkList(c)(addRowFieldNameExprs)

    val pivotRowClassDefExpr =
      createPivotRowClassDefTreeExpr(c)(weakTypeOf[SrcType], addRowFieldNames ++ preRowFieldNames)
    val pivotRowTypeInstanceExpr = c.Expr[PivotRow](
      Apply(Select(New(Ident(TypeName("PivotRowTypeClass"))), termNames.CONSTRUCTOR), Literal(Constant(null)) :: Nil))

    val remainingClassDefExpr = createRemainingTypeClassDefTreeExpr(c)(weakTypeOf[RemainingType], addRowFieldNames)

    reify {
      remainingClassDefExpr.splice
      pivotRowClassDefExpr.splice // add PivotRow class definition here since it should be in the same scope with new PivotTable[PivotRow, SrcType, KeyType]
      val pivotRowClassInstance = pivotRowTypeInstanceExpr.splice
      val src = c.prefix.splice
      val rowFields = newFieldNameExprs.splice.map(f => new RowField(f))
      new PivotTable[PivotRow, DynamicObject, SrcType](
        src.data,
        src.pivotFields,
        src.rowFields ++ rowFields,
        src.dataFields,
        pivotRowClassInstance)
    }

  }

  private def createPivotTableExprFromPivotOn[SrcType: c.WeakTypeTag, RemainingType: c.WeakTypeTag](c: Context {
    type PrefixType = Query[SrcType]
  })(fieldNamesSet: Set[String], fieldNames: List[c.Expr[String]]): c.Expr[Any] = {
    import c.universe._

    val remainingClassDefExpr = createRemainingTypeClassDefTreeExpr(c)(weakTypeOf[RemainingType], fieldNamesSet)
    val fieldNamesExpr: c.Expr[List[String]] = mkList(c)(fieldNames.toList)

    reify {
      remainingClassDefExpr.splice
      val src = c.prefix.splice
      val pivotFields = fieldNamesExpr.splice.map(f => {
        new PivotField(f)
      })
      new PivotTable[PivotRow, DynamicObject, SrcType](src, pivotFields, Nil, Nil, new DefaultPivotRow(Map.empty))
    }
  }

  private def createPivotTableExprFromAddPivotField[
      SrcType: c.WeakTypeTag,
      RowType <: DynamicObject: c.WeakTypeTag,
      RemainingType: c.WeakTypeTag](c: Context { type PrefixType = PivotTable[RemainingType, RowType, SrcType] })(
      fieldNamesSet: Set[String],
      fieldNames: List[c.Expr[String]]): c.Expr[Any] = {
    import c.universe._

    val remainingClassDefExpr = createRemainingTypeClassDefTreeExpr(c)(weakTypeOf[RemainingType], fieldNamesSet)
    val fieldNamesExpr: c.Expr[List[String]] = mkList(c)(fieldNames.toList)

    reify {
      remainingClassDefExpr.splice
      val src = c.prefix.splice
      val pivotFields = fieldNamesExpr.splice.map(f => {
        new PivotField(f)
      })
      new PivotTable[PivotRow, RowType, SrcType](
        src.data,
        src.pivotFields ++ pivotFields,
        src.rowFields,
        src.dataFields,
        src.rowFactory)
    }
  }

  private def createRemainingTypeClassDefTreeExpr(
      c: Context)(oldLeftGenericType: c.universe.Type, excludeNames: Set[String]): c.Expr[c.universe.Tree] = {
    import c.universe._

    // this is PivotRow template, later will add more methods into this class template
    val genericPivotRowClassTree = reify {
      class RemainingTypeClass(m: Map[String, Any]) extends PivotRow(m) {
        override def createPivotRow(m: Map[String, Any]) = new RemainingTypeClass(m)
      }
    }

    val fields = oldLeftGenericType.decls
      .collect {
        case d
            if d.isMethod && d.asMethod.paramLists.flatten.isEmpty
              && (d.asTerm.name != termNames.CONSTRUCTOR && d.asTerm.name.decodedName.toString != "map") =>
          d.asMethod
      }
      .filter(d => !excludeNames.contains(d.name.decodedName.toString))
      .toList
    val tpes = fields map { f =>
      f.returnType
    }

    val rowFieldDefs = fields zip tpes map { case (f, t) =>
      val fieldNameArg = Literal(Constant(f.name.decodedName.toString))
      val mapMethod = Apply(Select(Ident(TermName("map")), TermName("get")), List(fieldNameArg)) // map.get(fieldName)
      val typedMethod = TypeApply(
        Select(mapMethod, TermName("asInstanceOf")),
        List(TypeTree(t))
      ) // map.get(fieldName).asInstanceOf[fieldType]
      val rowFieldDef = DefDef(Modifiers(), TermName(f.name.decodedName.toString), Nil, Nil, TypeTree(t), typedMethod)
      rowFieldDef
    }

    // add fieldName and fieldType as DefDef into genericTree
    object xformForRowFieldDef extends Transformer {
      override def transform(tree: Tree): Tree = tree match {
        case Template(parents, self, body) =>
          treeCopy.Template(tree, parents, self, body ++ rowFieldDefs)
        case _ => super.transform(tree)
      }
    }
    val newGenericPivotRowClassTree = xformForRowFieldDef.transform(genericPivotRowClassTree.tree)
    val pivotRowClassDefTree = newGenericPivotRowClassTree.asInstanceOf[Block].stats.head.asInstanceOf[ClassDef]
    c.Expr[Tree](pivotRowClassDefTree)
  }

  private def createPivotRowClassDefTreeExpr(
      c: Context)(oldRGenericType: c.universe.Type, includeNames: Set[String]): c.Expr[c.universe.Tree] = {
    import c.universe._

    // this is PivotRow template, later will add more methods into this class template
    val genericPivotRowClassTree = reify {
      class PivotRowTypeClass(m: Map[String, Any]) extends PivotRow(m) {
        override def createPivotRow(m: Map[String, Any]) = new PivotRowTypeClass(m)
      }
    }

    val fields = oldRGenericType.decls
      .collect {
        case d if d.isMethod && d.asMethod.paramLists.flatten.isEmpty && (d.asTerm.name != termNames.CONSTRUCTOR) =>
          d.asMethod
      }
      .filter(d => includeNames.contains(d.name.decodedName.toString))
      .toList
    val tpes = fields map { f =>
      f.returnType
    }

    val rowFieldDefs = fields zip tpes map { case (f, t) =>
      val fieldNameArg = Literal(Constant(f.name.decodedName.toString))
      val mapMethod = Select(
        Apply(Select(Ident(TermName("map")), TermName("get")), List(fieldNameArg)),
        TermName("get")
      ) // map.get(fieldName).get
      val typedMethod = TypeApply(
        Select(mapMethod, TermName("asInstanceOf")),
        List(TypeTree(t))
      ) // map.get(fieldName).get.asInstanceOf[fieldType]
      val rowFieldDef = DefDef(Modifiers(), TermName(f.name.decodedName.toString), Nil, Nil, TypeTree(t), typedMethod)
      rowFieldDef
    }

    // add fieldName and fieldType as DefDef into genericTree
    object xformForRowFieldDef extends Transformer {
      override def transform(tree: Tree): Tree = tree match {
        case Template(parents, self, body) =>
          treeCopy.Template(tree, parents, self, body ++ rowFieldDefs)
        case _ => super.transform(tree)
      }
    }
    val newGenericPivotRowClassTree = xformForRowFieldDef.transform(genericPivotRowClassTree.tree)
    val pivotRowClassDefTree = newGenericPivotRowClassTree.asInstanceOf[Block].stats.head.asInstanceOf[ClassDef]

    c.Expr[Tree](pivotRowClassDefTree)
  }

  private def transformToNewPivotTableTreeWithLeftTypeClass(c: Context)(
      oldPivotTableTree: c.universe.Tree): c.universe.Tree = {
    import c.universe._

    // replace PivotRow with LeftTypeClass
    object xformForPivotRowGeneric extends Transformer {
      override def transform(tree: Tree): Tree = tree match {
        case New(atree @ AppliedTypeTree(tpt: Ident, args: List[_])) => {
          val newArgs: List[Tree] = args.map(arg => {
            arg match {
              case ident: Ident if (ident.name.decodedName.toString.equals("PivotRow")) =>
                Ident(TypeName("RemainingTypeClass"))
              case _ => arg
            }
          })
          val newTpt = treeCopy.AppliedTypeTree(atree, tpt, newArgs)
          treeCopy.New(tree, newTpt)
        }
        case _ => super.transform(tree)
      }
    }
    xformForPivotRowGeneric.transform(oldPivotTableTree)
  }

  private def transformToNewPivotTableTreeWithPivotRowTypeClass(c: Context)(
      oldPivotTableTree: c.universe.Tree): c.universe.Tree = {
    import c.universe._

    // replace PivotRow with PivotRowTypeClass
    object xformForPivotRowGeneric extends Transformer {
      override def transform(tree: Tree): Tree = tree match {
        case New(atree @ AppliedTypeTree(tpt: Ident, args: List[_])) => {
          val newArgs: List[Tree] = args.map(arg => {
            arg match {
              case ident: Ident if (ident.name.decodedName.toString.equals("DynamicObject")) =>
                Ident(TypeName("PivotRowTypeClass"))
              case _ => arg
            }
          })
          val newTpt = treeCopy.AppliedTypeTree(atree, tpt, newArgs)
          treeCopy.New(tree, newTpt)
        }
        case _ => super.transform(tree)
      }
    }
    xformForPivotRowGeneric.transform(oldPivotTableTree)
  }

  private def mkList[T](c: Context)(stats: List[c.Expr[T]]): c.Expr[List[T]] = {
    import c.universe._
    c.Expr[List[T]](q"_root_.scala.List.apply(..$stats)")
  }

  /**
   * pivotMap is used in PivotTable.map(src=> res) to convert all fields in res type to DataField of PivotTable. E.g.
   * pivotTable.map(items => new {val c1=items.map(_.amount).sum, val c2=...}), then it will convert all ValDef in the
   * anonymous class def to string lambda mapping and call addDataField: lambda = items => items.map(_.amount).sum
   * pivotTable.addDataField(DataField(c1, lambda))
   */
  def pivotMap[SrcType: c.WeakTypeTag, U: c.WeakTypeTag, R <: DynamicObject: c.WeakTypeTag, LeftType: c.WeakTypeTag](
      c: Context { type PrefixType = PivotTable[LeftType, R, SrcType] })(lambda: c.Expr[Query[SrcType] => U])(
      uType: c.Expr[TypeInfo[U]]): c.Expr[Any] = {

    import c.universe._
    import internal._

    /**
     * undo all macros applied to this tree and its sub tree
     */
    object WholeTreeUndoMacroExpansionTransformer extends Transformer {
      def undoOneMacro(t: c.Tree): Tree = {
        internal
          .attachments(t)
          .get[StdAttachments#MacroExpansionAttachment]
          .map(_.expandee.asInstanceOf[Tree])
          .getOrElse(t)
      }

      override def transform(tree: Tree): Tree = {
        val originalTree = undoOneMacro(tree)
        super.transform(originalTree)
      }
    }

    /**
     * reset the tree whose symbol is resetSymbol to NoSymbol, the tree is Ident which means it comes from outer scope
     * definition and we only reference it here e.g. t.map(f => f.position) here t comes from outer scope but not f
     */
    class ResetOuterAttributeSymbolTraverser(resetSymbol: Symbol) extends Traverser {
      override def traverse(tree: c.Tree) = tree match {
        case i: Ident if (i.symbol == resetSymbol) =>
          setSymbol(tree, NoSymbol)
        case _ => super.traverse(tree)
      }
    }

    /**
     * pick up all ValDefs in anonymous class def and generate DataField's name and lambda separately for every ValDef
     * e.g. new anno class { val posSum = q.map(t => t.position).sum } will become DataField("posSum", q => q.map(t =>
     * t.position).sum) so we need to generate function tree q => q.map(t => t.position).sum manually
     *
     * @param vparam
     *   the outer val def for Query type, e.g. q: Query[SrcType]
     */
    class ValTermTraverser(vparam: ValDef) extends Traverser {
      val colunmNames = new ListBuffer[c.Expr[String]]()
      val columnAssingments = new ListBuffer[c.Expr[Query[SrcType] => Any]]()

      def untypecheck(t: Tree) = {
        val localRemovedTree = c.untypecheck(t)
        // in 2.11 macro there is no 'resetAllAttrs' method but only 'untypecheck' which is to reset
        // local attrs (make these attrs' symbol and tpe to NoSymbol and null) but only to make global
        // attrs' tpe null. so we need to make global attrs' symbol NoSymbol manually
        new ResetOuterAttributeSymbolTraverser(vparam.symbol).traverse(localRemovedTree)
        localRemovedTree
      }

      override def traverse(tree: Tree) = tree match {
        case ValDef(mods, vname, tpt, rhs) => {
          val columnNameExpr = c.Expr[String](Literal(Constant(vname.decodedName.toString.trim())))
          colunmNames += columnNameExpr

          val newParam = ValDef(vparam.mods, vparam.name, vparam.tpt, vparam.rhs)
          val originalTree = WholeTreeUndoMacroExpansionTransformer.transform(rhs)
          val newOriginalTree = untypecheck(originalTree)
          val columnAssignmentExpr = c.Expr[Query[SrcType] => Any](Function(List(newParam), newOriginalTree))
          columnAssingments += columnAssignmentExpr
        }
        case _ => super.traverse(tree)
      }
    }

    val errorMsg =
      "Expression in PivotTable.map is not supported. Only anonymous classes can be returned, e.g. pivot map { items => new { val c = ??? } }"

    def addDataFields(tree: Tree) = tree match {
      case Function(vparam, Block(stats, Apply(_, _))) if (vparam.size == 1 && stats.size == 1) => {
        stats.head match {
          case ClassDef(mods, name, tparams, impl) =>
            val valTermTraverser = new ValTermTraverser(vparam.head)
            valTermTraverser.traverse(impl)

            // pivotTable.addDataField(columnNames(0), columnAssignments(0)).addDataField(columnNames(1), columnAssignments(1))......
            val combinedCallExpr = c.Expr[Any](
              valTermTraverser.colunmNames.zip(valTermTraverser.columnAssingments).foldLeft(c.prefix.tree) {
                case (preTree, (nameExpr, lambdaExpr)) =>
                  Apply(Select(preTree, TermName("addDataField")), List(nameExpr.tree, lambdaExpr.tree))
              }
            )

            reify {
              combinedCallExpr.splice
            }

          case _ =>
            c.error(c.enclosingPosition, errorMsg)
            null
        }
      }
      case _ =>
        c.error(c.enclosingPosition, errorMsg)
        null
    }
    addDataFields(lambda.tree)
  }
}
