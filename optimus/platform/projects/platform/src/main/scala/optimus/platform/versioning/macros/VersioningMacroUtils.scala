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
package optimus.platform.versioning.macros

import optimus.graph.PicklingMacroUtils
import optimus.platform._
import optimus.platform.versioning._
import optimus.platform.internal.MacroBase

import scala.collection.immutable.SortedMap
import scala.reflect.macros.blackbox.Context

private[optimus] trait VersioningMacroUtils[C <: Context] extends MacroBase with VersioningUtilsBase {
  val c: C
  type U = c.universe.type
  val u: U = c.universe

  import c.universe._
  import internal._

  protected object names {
    val apply = TermName("apply")
    val asInstanceOf = TermName("asInstanceOf")
    val get = TermName("get")
    val getName = TermName("getName")
    val graph = TermName("graph")
    val implicitly = TermName("implicitly")
    val info = TermName("info")
    val optimus = TermName("optimus")
    val pickle = TermName("pickle")
    val pickling = TermName("pickling")
    val platform = TermName("platform")
    val properties = TermName("properties")
    val storable = TermName("storable")
    val temporalContext = TermName("temporalContext")
    val txTime = TermName("unsafeTxTime")
    val unpickle = TermName("unpickle")
    val validTime = TermName("unsafeValidTime")
    val value = TermName("value")
    val transformerSteps = TermName("transformerSteps")
    val add = TermName("add")
    val rename = TermName("rename")
    val remove = TermName("remove")
    val change = TermName("change")
    val copyFields = TermName("copyFields")
    val copyUnpopulatedFields = TermName("unsafeCopyUnpopulatedFields")
    val valueOf = TermName("valueOf")
    val safeConstant = TermName("safeConstant")

    val any = TypeName("Any")
    val map = TypeName("Map")
  }

  protected object trees {

    private val PicklingMacroUtils = new PicklingMacroUtils[c.type](c)

    def setApply = Select(reify(Set).tree, names.apply)
    def shapeApply = Select(reify(RftShape).tree, names.apply)
    def mapApply = Select(reify(Map).tree, names.apply)
    def sortedMapApply = Select(reify(SortedMap).tree, names.apply)
    def tuple2Apply = Select(reify(Tuple2).tree, names.apply)
    def optimusGraph = Select(Ident(names.optimus), names.graph)
    def asNodeApply = Select(reify(asNode).tree, names.apply)
    def safeTransformerApply = Select(reify(SafeTransformer).tree, names.apply)
    def unsafeTransformerApply = Select(reify(UnsafeTransformer).tree, names.apply)
    def onewayTransformerApply = Select(reify(OnewayTransformer).tree, names.apply)
    def unsafeOnewayTransformerApply = Select(reify(UnsafeOnewayTransformer).tree, names.apply)

    def genConst(x: Any): c.universe.Literal = Literal(Constant(x))

    // Generate a read of a field, i.e. `properties(field)`
    def genFieldRead(propertiesSym: Symbol, field: Name): Tree = {
      val properties = Ident(propertiesSym.name)
      setSymbol(properties, propertiesSym)
      c.typecheck(Apply(Select(properties, names.apply), List(genConst(field.stringify))))
    }

    def genUnpickled(value: Tree, tpe: Type, pickledContext: Symbol, temporalContext: Symbol): Tree = {
      val pickledContextTree = Ident(pickledContext.name)
      setSymbol(pickledContextTree, pickledContext)
      val temporalContextRead = Ident(temporalContext.name)
      setSymbol(temporalContextRead, temporalContext)
      val unpickler = PicklingMacroUtils.selectRegistryUnpicklerMethod(tpe)
      val inputStream = q"optimus.platform.CoreAPIEx.pickledMapWrapper($pickledContextTree, $temporalContextRead)"
      val unpickled = Apply(Select(unpickler, names.unpickle), List(value, inputStream))
      unpickled
    }

    def genPickled(value: Tree, tpe: Type): Tree = {
      val pickler = PicklingMacroUtils.selectRegistryPicklerMethod(tpe)
      q"optimus.platform.CoreAPIEx.pickle($pickler, $value)"
    }

    def executeGivenTemporalContext(tree: Tree, temporalContext: Symbol): Tree = {
      val temporalContextRead = Ident(temporalContext.name)
      setSymbol(temporalContextRead, temporalContext)
      q"optimus.platform.CoreAPIEx.givenTC($temporalContextRead)($tree)"
    }

    private object registeredFieldTypes {
      lazy val arrayApply = Select(reify(RegisteredFieldType.Array).tree, names.apply)
      lazy val bigDecimal = reify(RegisteredFieldType.BigDecimal).tree
      lazy val boolean = reify(RegisteredFieldType.Boolean).tree
      lazy val businessEventReferenceApply = Select(reify(RegisteredFieldType.BusinessEventReference).tree, names.apply)
      lazy val byte = reify(RegisteredFieldType.Byte).tree
      lazy val char = reify(RegisteredFieldType.Char).tree
      lazy val collectionApply = Select(reify(RegisteredFieldType.Collection).tree, names.apply)
      lazy val compressedApply = Select(reify(RegisteredFieldType.Compressed).tree, names.apply)
      lazy val covariantSetApply = Select(reify(RegisteredFieldType.CovariantSet).tree, names.apply)
      lazy val double = reify(RegisteredFieldType.Double).tree
      lazy val duration = reify(RegisteredFieldType.Duration).tree
      lazy val embeddableApply = Select(reify(RegisteredFieldType.Embeddable).tree, names.apply)
      lazy val entityReferenceApply = Select(reify(RegisteredFieldType.EntityReference).tree, names.apply)
      lazy val float = reify(RegisteredFieldType.Float).tree
      lazy val immutableArrayApply = Select(reify(RegisteredFieldType.ImmutableArray).tree, names.apply)
      lazy val instant = reify(RegisteredFieldType.Instant).tree
      lazy val int = reify(RegisteredFieldType.Int).tree
      lazy val javaEnumApply = Select(reify(RegisteredFieldType.JavaEnum).tree, names.apply)
      lazy val knowableApply = Select(reify(RegisteredFieldType.Knowable).tree, names.apply)
      lazy val listMapApply = Select(reify(RegisteredFieldType.ListMap).tree, names.apply)
      lazy val localDate = reify(RegisteredFieldType.LocalDate).tree
      lazy val localTime = reify(RegisteredFieldType.LocalTime).tree
      lazy val long = reify(RegisteredFieldType.Long).tree
      lazy val mapApply = Select(reify(RegisteredFieldType.Map).tree, names.apply)
      lazy val msUnique = reify(RegisteredFieldType.MsUnique).tree
      lazy val msUuid = reify(RegisteredFieldType.MsUuid).tree
      lazy val chainedId = reify(RegisteredFieldType.ChainedID).tree
      lazy val offsetTime = reify(RegisteredFieldType.OffsetTime).tree
      lazy val optionApply = Select(reify(RegisteredFieldType.Option).tree, names.apply)
      lazy val orderedCollectionApply = Select(reify(RegisteredFieldType.OrderedCollection).tree, names.apply)
      lazy val period = reify(RegisteredFieldType.Period).tree
      lazy val productApply = Select(reify(RegisteredFieldType.Product).tree, names.apply)
      lazy val referenceHolderApply = Select(reify(RegisteredFieldType.ReferenceHolder).tree, names.apply)
      lazy val scalaEnumApply = Select(reify(RegisteredFieldType.ScalaEnum).tree, names.apply)
      lazy val setApply = Select(reify(RegisteredFieldType.Set).tree, names.apply)
      lazy val seqApply = Select(reify(RegisteredFieldType.Seq).tree, names.apply)
      lazy val short = reify(RegisteredFieldType.Short).tree
      lazy val sortedSetApply = Select(reify(RegisteredFieldType.SortedSet).tree, names.apply)
      lazy val string = reify(RegisteredFieldType.String).tree
      lazy val treeMapApply = Select(reify(RegisteredFieldType.TreeMap).tree, names.apply)
      lazy val tuple2Apply = Select(reify(RegisteredFieldType.Tuple2).tree, names.apply)
      lazy val tuple3Apply = Select(reify(RegisteredFieldType.Tuple3).tree, names.apply)
      lazy val tuple4Apply = Select(reify(RegisteredFieldType.Tuple4).tree, names.apply)
      lazy val tuple5Apply = Select(reify(RegisteredFieldType.Tuple5).tree, names.apply)
      lazy val tuple6Apply = Select(reify(RegisteredFieldType.Tuple6).tree, names.apply)
      lazy val tuple7Apply = Select(reify(RegisteredFieldType.Tuple7).tree, names.apply)
      lazy val tuple8Apply = Select(reify(RegisteredFieldType.Tuple8).tree, names.apply)
      lazy val tuple9Apply = Select(reify(RegisteredFieldType.Tuple9).tree, names.apply)
      lazy val tuple10Apply = Select(reify(RegisteredFieldType.Tuple10).tree, names.apply)
      lazy val unit = reify(RegisteredFieldType.Unit).tree
      lazy val unknownApply = Select(reify(RegisteredFieldType.Unknown).tree, names.apply)
      lazy val unorderedCollectionApply = Select(reify(RegisteredFieldType.UnorderedCollection).tree, names.apply)
      lazy val yearMonth = reify(RegisteredFieldType.YearMonth).tree
      lazy val year = reify(RegisteredFieldType.Year).tree
      lazy val zonedDateTime = reify(RegisteredFieldType.ZonedDateTime).tree
      lazy val zoneId = reify(RegisteredFieldType.ZoneId).tree
      lazy val fullName = reify(RegisteredFieldType.FullName).tree

    }

    def registeredFieldType(rft: RegisteredFieldType): Tree = rft match {
      case RegisteredFieldType.Unknown(additionalInfo) =>
        Apply(registeredFieldTypes.unknownApply, genConst(additionalInfo) :: Nil)
      case RegisteredFieldType.EntityReference(className) =>
        Apply(registeredFieldTypes.entityReferenceApply, genConst(className) :: Nil)
      case RegisteredFieldType.BusinessEventReference(className) =>
        Apply(registeredFieldTypes.businessEventReferenceApply, genConst(className) :: Nil)
      case RegisteredFieldType.Embeddable(className) =>
        Apply(registeredFieldTypes.embeddableApply, genConst(className) :: Nil)
      case RegisteredFieldType.ReferenceHolder(className) =>
        Apply(registeredFieldTypes.referenceHolderApply, genConst(className) :: Nil)
      case RegisteredFieldType.Boolean    => registeredFieldTypes.boolean
      case RegisteredFieldType.Byte       => registeredFieldTypes.byte
      case RegisteredFieldType.Char       => registeredFieldTypes.char
      case RegisteredFieldType.Double     => registeredFieldTypes.double
      case RegisteredFieldType.Float      => registeredFieldTypes.float
      case RegisteredFieldType.Int        => registeredFieldTypes.int
      case RegisteredFieldType.Long       => registeredFieldTypes.long
      case RegisteredFieldType.Short      => registeredFieldTypes.short
      case RegisteredFieldType.String     => registeredFieldTypes.string
      case RegisteredFieldType.Unit       => registeredFieldTypes.unit
      case RegisteredFieldType.BigDecimal => registeredFieldTypes.bigDecimal
      case RegisteredFieldType.MsUuid     => registeredFieldTypes.msUuid
      case RegisteredFieldType.MsUnique   => registeredFieldTypes.msUnique
      case RegisteredFieldType.ChainedID  => registeredFieldTypes.chainedId
      case RegisteredFieldType.FullName   => registeredFieldTypes.fullName
      case RegisteredFieldType.JavaEnum(className) =>
        Apply(registeredFieldTypes.javaEnumApply, genConst(className) :: Nil)
      case RegisteredFieldType.ScalaEnum(className) =>
        Apply(registeredFieldTypes.scalaEnumApply, genConst(className) :: Nil)
      case RegisteredFieldType.Product(className) =>
        Apply(registeredFieldTypes.productApply, genConst(className) :: Nil)
      case RegisteredFieldType.Tuple2(ft1, ft2) =>
        Apply(
          registeredFieldTypes.tuple2Apply,
          registeredFieldType(ft1) ::
            registeredFieldType(ft2) ::
            Nil)
      case RegisteredFieldType.Tuple3(ft1, ft2, ft3) =>
        Apply(
          registeredFieldTypes.tuple3Apply,
          registeredFieldType(ft1) ::
            registeredFieldType(ft2) ::
            registeredFieldType(ft3) ::
            Nil)
      case RegisteredFieldType.Tuple4(ft1, ft2, ft3, ft4) =>
        Apply(
          registeredFieldTypes.tuple4Apply,
          registeredFieldType(ft1) ::
            registeredFieldType(ft2) ::
            registeredFieldType(ft3) ::
            registeredFieldType(ft4) ::
            Nil)
      case RegisteredFieldType.Tuple5(ft1, ft2, ft3, ft4, ft5) =>
        Apply(
          registeredFieldTypes.tuple5Apply,
          registeredFieldType(ft1) ::
            registeredFieldType(ft2) ::
            registeredFieldType(ft3) ::
            registeredFieldType(ft4) ::
            registeredFieldType(ft5) ::
            Nil
        )
      case RegisteredFieldType.Tuple6(ft1, ft2, ft3, ft4, ft5, ft6) =>
        Apply(
          registeredFieldTypes.tuple6Apply,
          registeredFieldType(ft1) ::
            registeredFieldType(ft2) ::
            registeredFieldType(ft3) ::
            registeredFieldType(ft4) ::
            registeredFieldType(ft5) ::
            registeredFieldType(ft6) ::
            Nil
        )
      case RegisteredFieldType.Tuple7(ft1, ft2, ft3, ft4, ft5, ft6, ft7) =>
        Apply(
          registeredFieldTypes.tuple7Apply,
          registeredFieldType(ft1) ::
            registeredFieldType(ft2) ::
            registeredFieldType(ft3) ::
            registeredFieldType(ft4) ::
            registeredFieldType(ft5) ::
            registeredFieldType(ft6) ::
            registeredFieldType(ft7) ::
            Nil
        )
      case RegisteredFieldType.Tuple8(ft1, ft2, ft3, ft4, ft5, ft6, ft7, ft8) =>
        Apply(
          registeredFieldTypes.tuple8Apply,
          registeredFieldType(ft1) ::
            registeredFieldType(ft2) ::
            registeredFieldType(ft3) ::
            registeredFieldType(ft4) ::
            registeredFieldType(ft5) ::
            registeredFieldType(ft6) ::
            registeredFieldType(ft7) ::
            registeredFieldType(ft8) ::
            Nil
        )
      case RegisteredFieldType.Tuple9(ft1, ft2, ft3, ft4, ft5, ft6, ft7, ft8, ft9) =>
        Apply(
          registeredFieldTypes.tuple9Apply,
          registeredFieldType(ft1) ::
            registeredFieldType(ft2) ::
            registeredFieldType(ft3) ::
            registeredFieldType(ft4) ::
            registeredFieldType(ft5) ::
            registeredFieldType(ft6) ::
            registeredFieldType(ft7) ::
            registeredFieldType(ft8) ::
            registeredFieldType(ft9) ::
            Nil
        )
      case RegisteredFieldType.Tuple10(ft1, ft2, ft3, ft4, ft5, ft6, ft7, ft8, ft9, ft10) =>
        Apply(
          registeredFieldTypes.tuple10Apply,
          registeredFieldType(ft1) ::
            registeredFieldType(ft2) ::
            registeredFieldType(ft3) ::
            registeredFieldType(ft4) ::
            registeredFieldType(ft5) ::
            registeredFieldType(ft6) ::
            registeredFieldType(ft7) ::
            registeredFieldType(ft8) ::
            registeredFieldType(ft9) ::
            registeredFieldType(ft10) ::
            Nil
        )
      case RegisteredFieldType.Instant       => registeredFieldTypes.instant
      case RegisteredFieldType.Period        => registeredFieldTypes.period
      case RegisteredFieldType.ZonedDateTime => registeredFieldTypes.zonedDateTime
      case RegisteredFieldType.Duration      => registeredFieldTypes.duration
      case RegisteredFieldType.YearMonth     => registeredFieldTypes.yearMonth
      case RegisteredFieldType.Year          => registeredFieldTypes.year
      case RegisteredFieldType.LocalDate     => registeredFieldTypes.localDate
      case RegisteredFieldType.LocalTime     => registeredFieldTypes.localTime
      case RegisteredFieldType.OffsetTime    => registeredFieldTypes.offsetTime
      case RegisteredFieldType.ZoneId        => registeredFieldTypes.zoneId
      case RegisteredFieldType.OrderedCollection(elemType, collectionType) =>
        Apply(
          registeredFieldTypes.orderedCollectionApply,
          registeredFieldType(elemType) :: genConst(collectionType) :: Nil)
      case RegisteredFieldType.UnorderedCollection(elemType, collectionType) =>
        Apply(
          registeredFieldTypes.unorderedCollectionApply,
          registeredFieldType(elemType) :: genConst(collectionType) :: Nil)
      case RegisteredFieldType.Collection(elemType, collectionType) =>
        Apply(registeredFieldTypes.collectionApply, registeredFieldType(elemType) :: genConst(collectionType) :: Nil)
      case RegisteredFieldType.Compressed(t) =>
        Apply(registeredFieldTypes.compressedApply, registeredFieldType(t) :: Nil)
      case RegisteredFieldType.Option(t) =>
        Apply(registeredFieldTypes.optionApply, registeredFieldType(t) :: Nil)
      case RegisteredFieldType.Array(t) =>
        Apply(registeredFieldTypes.arrayApply, registeredFieldType(t) :: Nil)
      case RegisteredFieldType.ImmutableArray(t) =>
        Apply(registeredFieldTypes.immutableArrayApply, registeredFieldType(t) :: Nil)
      case RegisteredFieldType.Seq(t) =>
        Apply(registeredFieldTypes.seqApply, registeredFieldType(t) :: Nil)
      case RegisteredFieldType.Set(t) =>
        Apply(registeredFieldTypes.setApply, registeredFieldType(t) :: Nil)
      case RegisteredFieldType.CovariantSet(t) =>
        Apply(registeredFieldTypes.covariantSetApply, registeredFieldType(t) :: Nil)
      case RegisteredFieldType.SortedSet(t) =>
        Apply(registeredFieldTypes.sortedSetApply, registeredFieldType(t) :: Nil)
      case RegisteredFieldType.TreeMap(kt, vt) =>
        Apply(registeredFieldTypes.treeMapApply, registeredFieldType(kt) :: registeredFieldType(vt) :: Nil)
      case RegisteredFieldType.ListMap(kt, vt) =>
        Apply(registeredFieldTypes.listMapApply, registeredFieldType(kt) :: registeredFieldType(vt) :: Nil)
      case RegisteredFieldType.Map(kt, vt) =>
        Apply(registeredFieldTypes.mapApply, registeredFieldType(kt) :: registeredFieldType(vt) :: Nil)
      case RegisteredFieldType.Knowable(t) =>
        Apply(registeredFieldTypes.knowableApply, registeredFieldType(t) :: Nil)

    }
  }
}
