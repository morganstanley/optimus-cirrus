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

import optimus.tools.scalacplugins.entity.reporter.OptimusAlarmBuilder0
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.immutable.HashMap
import scala.tools.nsc.symtab.Flags

trait UntypedUtils extends PluginUtils {
  import CODE._
  import global._

  // definition of Optimus Classes in AST format, use uppercase to indicate they represent classes
  def EntityInfo = Select(EntityPkg, tpnames.EntityInfo)
  def ClassEntityInfo = Select(EntityPkg, tpnames.ClassEntityInfo)
  def EntityReference = Select(StorablePkg, tpnames.EntityReference)
  def PropertyInfo = Select(GraphPkg, tpnames.PropertyInfo)
  def DefPropertyInfo(numOfArgs: Int, noTweaks: Boolean) =
    Select(GraphPkg, newTypeName((if (noTweaks) "PropertyInfo" else "TwkPropertyInfo") + numOfArgs.toString))
  def BaseIndexPropertyInfo = Select(GraphPkg, tpnames.BaseIndexPropertyInfo)
  def IndexPropertyInfoForErefFilter = Select(GraphPkg, tpnames.IndexPropertyInfoForErefFilter)
  def UniqueIndexPropertyInfo = Select(GraphPkg, tpnames.UniqueIndexPropertyInfo)
  def EventIndexPropertyInfo = Select(GraphPkg, tpnames.EventIndexPropertyInfo)
  def EventUniqueIndexPropertyInfo = Select(GraphPkg, tpnames.EventUniqueIndexPropertyInfo)
  def NodeTaskInfo = Select(GraphPkg, tpnames.NodeTaskInfo)
  def AppliedStorageInfo = StorablePkg DOT names.AppliedStorageInfo tree
  def UniqueStorageInfo = StorablePkg DOT names.UniqueStorageInfo tree
  def PickledMapWrapper = Select(PicklingPkg, tpnames.PickledMapWrapper)
  def Instant = Select(JavaTimePkg, tpnames.Instant)
  def UnsupportedOperationException = Select(JavaLangPkg, tpnames.UnsupportedOperationException)

  lazy val AsyncAnnotation = Select(PlatformPkg, tpnames.async)
  lazy val ImpureAnnotation = Select(PlatformPkg, tpnames.impure)
  lazy val NodeSyncAnnotationType = Select(AnnotationPkg, tpnames.nodeSync)
  lazy val MiscFlagsAnnotationType = Select(AnnotationPkg, tpnames.miscFlags)
  lazy val TweakableAnnotationType = Select(AnnotationPkg, tpnames.tweakable)
  lazy val NonTweakableAnnotationType = Select(AnnotationPkg, tpnames.nonTweakable)
  lazy val StoredAnnotation = Select(PlatformPkg, tpnames.stored)
  lazy val LoomAnnotation = Select(PlatformPkg, tpnames.loom)
  lazy val GetterAnnotation = Select(ScalaMetaAnnotationPkg, tpnames.getter)
  lazy val FieldAnnotation = Select(ScalaMetaAnnotationPkg, tpnames.field)
  lazy val EntersGraphAnnotation = Select(PlatformPkg, tpnames.entersGraph)
  lazy val TransientAnnotation = Select(ScalaPkg, tpnames.transient)
  lazy val AutoGenCreationAnnotationType = Select(AnnotationPkg, tpnames.autoGenCreation)
  lazy val C2PAnnotation = Select(AnnotationPkg, tpnames.c2p)
  lazy val EntityMetaDataAnnotation = Select(InternalAnnotationPkg, tpnames.EntityMetaDataAnnotation)
  lazy val EmbeddableMetaDataAnnotation = Select(InternalAnnotationPkg, tpnames.EmbeddableMetaDataAnnotation)
  lazy val AccelerateInfoAnnotation = Select(InternalAnnotationPkg, tpnames.AccelerateInfoAnnotation)

  lazy val Pickler = Select(PicklingPkg, tpnames.Pickler)
  lazy val PickledInputStream = Select(PicklingPkg, tpnames.PickledInputStream)
  lazy val QueryTemporality = Select(DalPkg, tpnames.QueryTemporality)
  lazy val KeyType = Select(StorablePkg, tpnames.Key)
  lazy val OptionType = Select(ScalaPkg, tpnames.Option)

  object PluginMacros {
    def THIS = Select(InternalPkg, names.PluginMacros)
    def PICKLING = Select(PicklingPkg, newTermName("PicklingMacros"))
    def AnyType = gen.rootScalaDot(tpnme.Any)

    object localNames {
      val findPickler = newTermName("findPickler")
      val findInheritPropertyInfoForCompoundKey = newTermName("findInheritPropertyInfoForCompoundKey")
      val getIsCollection = newTermName("getIsCollection")
    }

    def findPickler(tree: Tree) = Apply(Select(PICKLING, localNames.findPickler), List(tree))
    def findInheritPropertyInfoForCompoundKey(cd: ClassDef, name: TermName) =
      Apply(
        TypeApply(Select(THIS, localNames.findInheritPropertyInfoForCompoundKey), mkETypeOf(cd) :: Nil),
        Function(
          List(ValDef(Modifiers(Flags.PARAM | Flags.SYNTHETIC), newTermName("x$1"), TypeTree(), EmptyTree)),
          Select(Ident(newTermName("x$1")), name)) :: Nil
      )
    def getIsCollection(cd: ClassDef, name: TermName): Apply =
      Apply(
        TypeApply(Select(THIS, localNames.getIsCollection), mkETypeOf(cd) :: Nil),
        Function(
          List(ValDef(Modifiers(Flags.PARAM | Flags.SYNTHETIC), newTermName("x$1"), TypeTree(), EmptyTree)),
          Select(Ident(newTermName("x$1")), name)) :: Nil
      )
  }

  /**
   * Tests a given tree to match [new optimus.platform.node] it matches name and structure only! Which means it could be
   * entirely different node attribute from a different package TODO (OPTIMUS-0000): We should either add logic to resolve fully
   * qualified name (hard) or at least verify after typer
   */
  def isSpecificAnnotation(tree: Tree, expectedType: TypeName): Boolean = {
    @tailrec
    def isOptimusPlatform(qual: Tree): Boolean = {
      qual match {
        /// case p if p == PlatformPkg => true
        case Select(Ident(names.optimus), names.platform)                      => true
        case Select(Select(Ident(nme.ROOTPKG), names.optimus), names.platform) => true
        case Select(qual, _)                                                   => isOptimusPlatform(qual)
        case _                                                                 => false
      }
    }

    tree match {
      case Apply(Select(New(Ident(actualType)), nme.CONSTRUCTOR), _) if expectedType == actualType               => true
      case Apply(Select(New(Annotated(_, Ident(actualType))), nme.CONSTRUCTOR), _) if expectedType == actualType => true
      case Apply(Select(New(Select(qual, actualType)), nme.CONSTRUCTOR), _)
          if (expectedType == actualType) && isOptimusPlatform(qual) =>
        true
      case _ => false
    }
  }

  /**
   * Assumes simple literal arguments
   */
  def getAnnotationArgs(tree: MemberDef, expectedType: TypeName): Map[Name, Any] = {
    var args = new HashMap[Name, Any]
    var cur = tree.mods.annotations
    while (cur.nonEmpty) {
      cur.head match {
        case Apply(Select(New(Ident(actualType)), nme.CONSTRUCTOR), argTrees) if expectedType == actualType =>
          for (NamedArgTree(Ident(name), Literal(Constant(value))) <- argTrees) {
            args += name -> value
          }
          cur = Nil
        case _ => cur = cur.tail
      }
    }
    args
  }

  @tailrec
  private def miscFlags(as: List[Tree]): Int = as match {
    case Apply(
          Select(New(Ident(tpnames.miscFlags)), nme.CONSTRUCTOR),
          NamedArgTree(Ident(names.flags), Literal(Constant(value))) :: Nil) :: _ =>
      value.asInstanceOf[Int]
    case _ :: as => miscFlags(as)
    case Nil     => 0
  }
  def miscFlags(tree: ValOrDefDef): Int = miscFlags(tree.mods.annotations)
  def hasMiscFlag(tree: ValOrDefDef, flag: Int): Boolean = (miscFlags(tree) & flag) != 0
  def withMiscFlag(annotations: List[Tree], flag: Int): List[Tree] = {
    val flags = miscFlags(annotations)
    val filteredAnnots =
      if (flags != 0)
        annotations.filterNot(isSpecificAnnotation(_, tpnames.miscFlags))
      else
        annotations
    val newMiscAnnot =
      mkAppliedAnnotation(
        MiscFlagsAnnotationType,
        NamedArgTree(Ident(names.flags), Literal(Constant(flags | flag))) :: Nil)
    newMiscAnnot :: filteredAnnots
  }
  def withMiscFlag(mods: Modifiers, flag: Int): Modifiers =
    mods.copy(annotations = withMiscFlag(mods.annotations, flag))

  def hasTweak(mods: Modifiers): Boolean = getBooleanAnnoParam(tpnames.node, names.tweak, mods).getOrElse(false)

  def hasExposeArgTypes(mods: Modifiers): Boolean = hasExposeArgTypesOnNode(mods) || hasExposeArgTypesOnAsync(mods)
  def hasExposeArgTypesOnNode(mods: Modifiers): Boolean = hasExposeArgTypesOnAnno(mods, tpnames.node)
  def hasExposeArgTypesOnAsync(mods: Modifiers): Boolean = hasExposeArgTypesOnAnno(mods, tpnames.async)
  private def hasExposeArgTypesOnAnno(mods: Modifiers, annoName: TypeName): Boolean =
    getBooleanAnnoParam(annoName, names.exposeWithArgTypes, mods).getOrElse(false)

  def getProjectedParam(anno: Tree): Option[Boolean] = getBooleanAnnoParam(anno, names.projected)
  def getFullTextSearchParam(anno: Tree): Option[Boolean] = getBooleanAnnoParam(anno, names.fullTextSearch)
  def getSchemaVersionParam(anno: Tree): Option[Int] = getIntAnnoParam(anno, names.schemaVersion)

  def getStoredProjectedParam(mods: Modifiers): Option[Boolean] =
    getBooleanAnnoParam(tpnames.stored, names.projected, mods)

  def hasAnyAnnotation(mods: Modifiers, annotations: Seq[TypeName]): Boolean =
    annotations.exists(hasAnnotation(mods, _))
  def hasAnnotation(mods: Modifiers, tpe: TypeName): Boolean = mods.annotations.exists(isSpecificAnnotation(_, tpe))
  def getAnnotation(mods: Modifiers, tpe: TypeName): Option[Tree] = mods.annotations.find(isSpecificAnnotation(_, tpe))

  def withoutAnnotations(mods: Modifiers, annots: Seq[TypeName]): Modifiers =
    mods.copy(annotations = mods.annotations.filterNot { a =>
      annots.exists(isSpecificAnnotation(a, _))
    })

  private def getIntAnnoParam(tree: Tree, name: TermName): Option[Int] =
    getAnnoParam[Int](tree, name)(_.intValue)

  private def getBooleanAnnoParam(ann: TypeName, prop: TermName, mods: Modifiers): Option[Boolean] =
    getAnnotation(mods, ann).flatMap(getBooleanAnnoParam(_, prop))

  private def getBooleanAnnoParam(tree: Tree, name: TermName): Option[Boolean] =
    getAnnoParam[Boolean](tree, name)(_.booleanValue)

  private def getAnnoParam[T: Manifest](tree: Tree, name: TermName)(converter: Constant => T): Option[T] =
    tree match {
      case Apply(Select(_, nme.CONSTRUCTOR), Literal(c: Constant) :: Nil) => Some(converter(c))
      case Apply(Select(_, nme.CONSTRUCTOR), args) =>
        args.collectFirst { case NamedArgTree(Ident(`name`), Literal(c: Constant)) => converter(c) }
      case _ => None
    }

  def isEventProjected(mods: Modifiers): Boolean =
    getBooleanAnnoParam(tpnames.event, names.projected, mods).getOrElse(false)

  def isEventContained(mods: Modifiers): Boolean =
    getBooleanAnnoParam(tpnames.event, names.contained, mods).getOrElse(false)

  def isEmbeddableProjected(mods: Modifiers): Boolean =
    getBooleanAnnoParam(tpnames.embeddable, names.projected, mods).getOrElse(false)

  def isProjectionIndexed(mods: Modifiers): Boolean =
    getBooleanAnnoParam(tpnames.projected, names.indexed, mods).getOrElse(false)

  def isProjectionQueryable(mods: Modifiers): Boolean =
    getBooleanAnnoParam(tpnames.projected, names.queryable, mods).getOrElse(true)

  def isEntity(mods: Modifiers): Boolean = hasAnnotation(mods, tpnames.entity)
  def isExecuteColumn(mods: Modifiers): Boolean = hasAnnotation(mods, tpnames.executeColumn)
  def isEvent(mods: Modifiers): Boolean = hasAnnotation(mods, tpnames.event)
  def isStored(mods: Modifiers): Boolean = isEntity(mods) && hasAnnotation(mods, tpnames.stored)
  def isKey(mods: Modifiers): Boolean = hasAnnotation(mods, tpnames.key)
  def isIndex(mods: Modifiers): Boolean = hasAnnotation(mods, tpnames.indexed)
  def isStoredC2P(mods: Modifiers): Boolean =
    getBooleanAnnoParam(tpnames.stored, names.childToParent, mods).getOrElse(false)

  def isKeyOrIndex(mods: Modifiers): Boolean = isKey(mods) || isIndex(mods)
  def isUniqueIndex(mods: Modifiers): Boolean =
    getBooleanAnnoParam(tpnames.indexed, names.unique, mods).getOrElse(false)

  def isQueryableIndex(mods: Modifiers): Boolean =
    getBooleanAnnoParam(tpnames.indexed, names.queryable, mods).getOrElse(true)

  def isQueryableByRefOnly(mods: Modifiers): Boolean =
    getBooleanAnnoParam(tpnames.indexed, names.queryByEref, mods).getOrElse(false)

  // [[JOB_EXPERIMENTAL]]
  def isNonEntityObject(implDef: ImplDef): Boolean =
    (implDef ne null) && implDef.isInstanceOf[ModuleDef] && !isEntity(implDef.mods)

  def hasNodeOrAsyncAnno(vd: DefDef): Boolean =
    hasNodeAnno(vd.mods) || hasAnnotation(vd.mods, tpnames.async)

  def isTransient(mods: Modifiers): Boolean =
    mods.hasAnnotationNamed(tpnames.transient) // transient is not in optimus.platform package

  def isFullTextSearch(mods: Modifiers): Boolean =
    hasAnnotation(mods, tpnames.fullTextSearch)

  def transToUniqueUniverse(tree: Tree): Block = {
    val resName = names.tmp
    val valdef = ValDef(NoMods, resName, TypeTree(), tree)
    val setApplied = Apply(Select(Ident(resName), newTermName("mkUnique")), Nil)
    Block(List(valdef, setApplied), Ident(resName))
  }

  def uncheckedVariance(tpt: Tree): Tree = tpt match {
    case _: SingletonTypeTree => tpt // avoid to add @uncheckedVariance to Object.type
    case other                => Annotated(mkUncheckedVarianceAnnotation, other)
  }

  def mkUncheckedVarianceAnnotation: Tree = New(definitions.uncheckedVarianceClass, Nil: _*)
  def addAutoGenCreationAnnotationToMods(mods: Modifiers, pos: Position): Modifiers =
    mods withAnnotations mkAnnotation(AutoGenCreationAnnotationType, pos) :: Nil

  def mkAnnotation(annoTpe: Tree, pos: Position): Apply = atPos(pos.focusStart) { mkAppliedAnnotation(annoTpe, Nil) }
  def mkAppliedAnnotation(annoTpe: Tree, args: List[Tree]): Apply = {
    Apply(Select(New(annoTpe), nme.CONSTRUCTOR), args)
  }

  // only returns a tree if the condition is true
  def mkConditionalNamedArgTree(name: TermName, condition: Boolean): Option[Tree] = {
    if (condition) Some(mkNamedArgTree(name, condition))
    else None

  }
  def mkOptionalNamedArgTree(name: TermName, value: Option[Any]): Option[Tree] = value.map(mkNamedArgTree(name, _))
  def mkNamedArgTree(name: TermName, value: Any): Tree = NamedArgTree(Ident(name), Literal(Constant(value)))

  /**
   * Takes a type tree (tpt) and wraps it in supplied type constructor if tpt is not empty
   */
  def applyType(tctor: Tree, tpt: Tree*): Tree = tpt match {
    case Seq()           => TypeTree()
    case Seq(TypeTree()) => TypeTree()
    case _               => AppliedTypeTree(tctor, tpt.toList map { dupTree(_) setPos NoPosition })
  }

  // existential type references
  def mkETypeOf(implDef: Tree): Tree = implDef match {
    case cd: ClassDef =>
      if (cd.tparams.isEmpty) Ident(cd.name)
      else {
        val tpnames = cd.tparams map { td =>
          Ident(td.name)
        }
        ExistentialTypeTree(AppliedTypeTree(Ident(cd.name), tpnames), dupTree(cd.tparams))
      }
    case md: ModuleDef => SingletonTypeTree(Ident(md.name))
  }

  /**
   * given a ClassDef or ModuleDef X, generates classOf[X] (with suitable upper bounds for any type params, to make the
   * compiler happy)
   */
  def mkClassOf(implDef: Tree): Tree = TypeApply(Ident(nme.classOf), mkETypeOf(implDef) :: Nil)

  // concrete type references
  def mkCTypeOf(implDef: Tree): Tree = implDef match {
    case cd: ClassDef  => typedRef(cd.name, cd.tparams)
    case md: ModuleDef => SingletonTypeTree(Ident(md.name))
  }

  def mkUBoundTypes(srcTparams: List[TypeDef], useTparams: List[Tree]): List[Tree] =
    if (srcTparams.isEmpty) useTparams
    else {
      // first get upper bounds for each type param
      val typeNameToUB = srcTparams map { td =>
        td.name -> td.rhs.asInstanceOf[TypeBoundsTree].hi
      } toMap

      // Need to iterate to fixed point in replacing typeNames with upper bound trees as the upper bound
      // trees themselves can contain references to the free typenames.
      // See MultilevelTypeParameterDependence.scala compilation test.
      @tailrec
      def normalize(td: Tree): Tree = {
        val hasFreeTypeNames = td exists {
          case Ident(name: TypeName)  => typeNameToUB.contains(name)
          case TypeDef(_, name, _, _) => typeNameToUB.contains(name)
          case _                      => false
        }

        if (hasFreeTypeNames)
          normalize(replaceTypes(td, typeNameToUB))
        else
          replaceTypes(td, typeNameToUB)
      }

      // now substitute those bounds in wherever the type param appears
      val tpnames = useTparams map { td =>
        normalize(td)
      }
      tpnames
    }

  def mkUBoundTypeOf(implDef: ImplDef): Tree = implDef match {
    case cd: ClassDef =>
      if (cd.tparams.isEmpty) Ident(cd.name)
      else {
        AppliedTypeTree(Ident(cd.name), mkUBoundTypes(cd.tparams, cd.tparams))
      }
    case md: ModuleDef => SingletonTypeTree(Ident(md.name))
  }

  // traverses a type tree such as this example (or simpler / more complex cases)...
  //
  // example: "F <: Foo[T]"       (e.g. where "T <: Number" is another type parameter)
  //
  // TypeDef(Modifiers(PARAM), newTypeName("F"), List.empty,
  //    TypeBoundsTree(
  //      Ident(scala.Nothing),
  //      AppliedTypeTree(Ident(Foo),
  //        List(Ident(newTypeName("T"))
  //      ))
  //    )
  //  )
  //
  // ...replacing the type parameters with the supplied upper bounds to get "Foo[Number]" instead
  //
  private def replaceTypes(tr: Tree, upperBounds: Map[TypeName, Tree]): Tree = tr match {
    case EmptyTree => gen.mkAttributedIdent(definitions.AnyClass)
    case Ident(name: TypeName) if upperBounds.contains(name) =>
      dupTree(upperBounds(name)) // replace bounded types with upper bound
    case TypeDef(_, _, _, rhs) => replaceTypes(rhs, upperBounds) // substitute any type def for its RHS
    case TypeBoundsTree(_, hi) => replaceTypes(hi, upperBounds) // substitute any bounds for the upper bound
    case AppliedTypeTree(t, args) =>
      AppliedTypeTree(t, args.map(replaceTypes(_, upperBounds))) // replace type args with upper bound
    case ExistentialTypeTree(t, clauses) =>
      ExistentialTypeTree(replaceTypes(t, upperBounds), clauses) // replace types in root of existential type
    case CompoundTypeTree(Template(parents, self, body)) =>
      // Replace types parents in compound types (e.g. A with B). Currently we do not transform body of class (e.g. def declarations)
      CompoundTypeTree(Template(parents.map(tree => replaceTypes(tree, upperBounds)), self, body))
    case _ => tr
  }

  // Replaces type with upper bound
  // TODO (OPTIMUS-0000): make it recursive
  def mkUBoundTypeOf(implDef: Tree, tpt: Tree): Tree = implDef match {
    case cd: ClassDef =>
      if (cd.tparams.isEmpty) tpt
      else {
        tpt match {
          case Ident(name) =>
            val td = cd.tparams.find(_.name == name)
            if (td.isDefined)
              td.get.rhs.asInstanceOf[TypeBoundsTree].hi
            else tpt
          case _ => tpt
        }

      }
    case _: ModuleDef => tpt
  }

  case class TweakInfo(isTweakable: Boolean, isDefault: Boolean)
  protected def getNodeTweakOnSetting(vodd: ValOrDefDef): Boolean = getNodeTweakInfoOnSetting(vodd).isTweakable
  protected def getNodeTweakInfoOnSetting(vodd: ValOrDefDef): TweakInfo = {
    if (!hasNodeAnno(vodd.mods))
      TweakInfo(isTweakable = false, isDefault = true)
    else {
      val args = getAnnotationArgs(vodd, tpnames.node)
      val tweakArg = args.get(names.tweak)
      tweakArg match {
        case Some(b: Boolean) => TweakInfo(b, isDefault = false)
        case _ =>
          TweakInfo(isTweakable = false, isDefault = true) // Really it's an error, but the typer will flag it later
      }
    }
  }

  def hasNodeAnno(mods: Modifiers): Boolean = hasAnnotation(mods, tpnames.node)

  def isScalaFunction(tree: Tree): Boolean = {
    def nameMatches(func: String, args: List[Tree]): Boolean = func == s"Function${args.size - 1}"
    tree match {
      case AppliedTypeTree(Select(Select(Ident(nme.ROOTPKG), nme.scala_) | Ident(nme.scala_), TypeName(func)), args) =>
        // e.g. A => B, _root_.scala.Function1[A, B], scala.Function1[A, B]
        nameMatches(func, args)
      case AppliedTypeTree(Ident(TypeName(func)), args) =>
        // e.g. Function1[A, B]
        nameMatches(func, args)
      case _ => false
    }
  }
}
