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
package optimus.graph

import optimus.entity.IndexInfo
import optimus.platform.DAL.resolver
import optimus.platform._
import optimus.platform.annotations.nodeSync
import optimus.platform.dal._
import optimus.platform.dsi.bitemporal.proto.ProtoSerialization
import optimus.platform.internal.OptimusReporter
import optimus.platform.pickling._
import optimus.platform.relational.LambdaReifier
import optimus.platform.relational.PostMacroRewriter
import optimus.platform.relational.tree._
import optimus.platform.storable._
import optimus.platform.temporalSurface.operations.QueryByClass
import optimus.platform.versioning.RegisteredFieldType
import optimus.platform.versioning.macros.VersioningMacroUtils
import optimus.tools.scalacplugins.entity.reporter.DALAlarms

import scala.annotation.StaticAnnotation
import scala.reflect.macros.blackbox.Context

/**
 * Helper functions for scalac entity plugin. See [[optimus.graph.CorePluginSupport]] for lower level (ie, not requiring
 * platform._) supporting methods and more documentation.
 */
object PluginSupport {
  def genProjectedMemberMacro[E: c.WeakTypeTag, T: c.WeakTypeTag](
      c: Context)(name: c.Expr[String], indexed: c.Expr[Boolean], queryable: c.Expr[Boolean], f: c.Expr[E => T]) = {
    new GenProjectedMemberMacros[c.type](c).genProjectedMember[E, T](name, indexed, queryable, f)
  }

  def genReifiedLambdaMacro[T: c.WeakTypeTag](c: Context)(f: c.Expr[T]): c.Expr[LambdaElement] = {
    val lam = new LambdaReifier[c.type](c).reify(f.tree)
    c.universe.reify { PostMacroRewriter.rewrite(lam.splice) }
  }

  def findEntitiesWithErefFilterMacro[E <: Entity: c.WeakTypeTag, R: c.WeakTypeTag](
      c: Context)(keyValue: c.Expr[R], entityIter: c.Expr[Iterable[E]]): c.Expr[Iterable[E]] = {
    import c.universe._
    val callerPrefixExpr = c.prefix.asInstanceOf[Expr[BaseIndexPropertyInfo[E, R]]]
    val keyExpr = reify(callerPrefixExpr.splice.makeKey(keyValue.splice))
    val filterCondExpr = reify { (e: E) =>
      e.getClass.getMethod(callerPrefixExpr.splice.name()).invoke(e) == keyValue.splice
    }

    entityIter.tree match {
      case t @ Select(entity, entityPropName) if entity.tpe <:< typeOf[Entity] =>
        val entityPropNodeKeyExpr =
          c.Expr[NodeKey[_]](q"optimus.platform.nodeKeyOf($entity.${entityPropName.toTermName})")
        val loadContextExpr = c.Expr[TemporalContext](Select(entity, TermName("dal$temporalContext")))
        reify(
          findEntitiesWithErefFilterImpl(
            keyExpr.splice,
            entityPropNodeKeyExpr.splice,
            filterCondExpr.splice,
            loadContextExpr.splice))
      case _ => reify(entityIter.splice filter filterCondExpr.splice)
    }
  }

  @node @scenarioIndependent
  def findEntitiesWithErefFilterImpl[E <: Entity, PropType](
      key: Key[E],
      keyNode: NodeKey[_],
      filterCond: E => Boolean,
      loadContext: TemporalContext): Iterable[E] = keyNode match {
    case node: MaybePickledReference[_] =>
      // read pickled once - it can get cleared at any time
      val p = node.pickled
      if (p != null)
        DAL.resolver.findByIndexWithEref(key, getEntityReferenceFromPickledRepr(node, p), loadContext).toSeq
      else handleNode(node).asInstanceOf[Seq[E]].filter(filterCond)
    case cnode: AlreadyCompletedPropertyNode[_] => handleNode(cnode).asInstanceOf[Seq[E]].filter(filterCond)
    // illegal cases
    case badNode: PropertyNode[_] =>
      throw new IllegalStateException(s"Unexpected node type $badNode ${badNode.getClass}")
  }

  abstract class InferredMetaKeyBase[E <: Storable, K](
      n: String,
      ns: Seq[String],
      f: E => K,
      u: Boolean,
      i: Boolean,
      d: Boolean,
      isCollection: Boolean
  ) extends IndexInfo[E, K](n, ns, u, i, d, isCollection) {
    private def isExpandedIndex: Boolean = isCollection && indexed && !unique

    /** serializes a key/index value */
    final def serialize(res: K, referenceMap: collection.Map[Entity, EntityReference]): Seq[SerializedKey] = {
      val decoratedRes =
        if (isExpandedIndex && !res.isInstanceOf[Iterable[_]] && !res.isInstanceOf[Array[_]])
          Seq(res).asInstanceOf[K]
        else res
      val out = new PropertyMapOutputStream(referenceMap)
      out.writeStartArray()
      pickleRep(decoratedRes, out)
      out.writeEndArray()
      val props = out.value.asInstanceOf[Iterable[_]]
      require(props.size == ns.size)
      makeSerializedKeys(ns zip props)
    }

    /** extracts key/index value from an entity and serializes it */
    final def entityToSerializedKey(e: E, referenceMap: collection.Map[Entity, EntityReference]): Seq[SerializedKey] = {
      val fieldInfos = e.$info.unsafeFieldInfo
      val fieldValues = ns.map { name =>
        val out = new PropertyMapOutputStream(referenceMap)
        fieldInfos.find(_.pinfo.name == name) match {
          case Some(fieldInfo) =>
            // the index field corresponds to a property, so use the standard property pickling mechanism which
            // optimises for the case of unhydrated lazy entity references (by avoiding loading the referenced entity)
            ReflectivePicklingImpl.writePropertyValue(e, fieldInfo, out)
          case None =>
            // the index field doesn't correspond to a property. This is typically something like
            // @indexed def idx = embeddable.field - not currently optimizable, so just pickle the result of f
            // TODO (OPTIMUS-65664): Optimize non-property-based indexes
            pickleRep(f(e), out)
        }
        name -> out.value
      }

      makeSerializedKeys(fieldValues)
    }

    private def makeSerializedKeys(fieldValues: Seq[(String, Any)]): Seq[SerializedKey] = {
      if (isExpandedIndex) {
        val (fieldName, fieldValue) = fieldValues.single
        ProtoSerialization
          .distinctBy(fieldValue.asInstanceOf[Iterable[Any]].iterator, identity[Any])
          .map(e => SerializedKey(storableClass.getName, Seq(fieldName -> e), unique, indexed, queryByEref))
          .toIndexedSeq
      } else
        Seq(SerializedKey(storableClass.getName, fieldValues, unique, indexed, queryByEref))
    }

    final def pickle(e: E, out: PickledOutputStream): Unit = {
      val res = f(e)
      pickleRep(res, out)
    }

    protected def pickleRep(res: K, out: PickledOutputStream): Unit

    protected def stringForPrettyPrint(k: K): String = k.toString

    final override def getRaw(e: E): Any = f(e)

    final override def entityToKeyImpl(entity: E): Key[E] = KeyImpl(f(entity))
    final override def KeyImpl(p: K): Key[E] = if (u) new UniqueKeyImpl(p) else new NonUniqueKeyImpl(p)

    protected def targetClass: Class[E] = storableClass

    sealed trait KeyImpl extends Key[E] {
      protected val p: K
      override def toString = s"${targetClass.getName}.${name}${stringForPrettyPrint(p)}"
      final val toSerializedKey = {
        val key = serialize(p, Map.empty)
        require(key.size == 1, "cannot query using collection")
        key.head
      }
      final def subjectClass: Class[E] = targetClass
    }
    class UniqueKeyImpl(protected val p: K) extends UniqueKey[E] with KeyImpl
    class NonUniqueKeyImpl(protected val p: K) extends NonUniqueKey[E] with KeyImpl
  }

  // Key implementation that uses a function returning a TupleN defined on the user-written class.
  class InferredMetaKey[E <: Storable, K <: Product](
      name: String,
      ns: Seq[String],
      f: E => K,
      picklers: => Seq[Pickler[_]],
      uniq: Boolean,
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      val storableClass: Class[E])
      extends InferredMetaKeyBase[E, K](name, ns, f, uniq, indexed, default, isCollection) {

    override protected def pickleRep(res: K, out: PickledOutputStream): Unit = {
      require(picklers.length == res.productArity)
      val it = res.productIterator

      picklers foreach { pickler =>
        out.write(it.next(), pickler.asInstanceOf[Pickler[Any]])
      }
    }

    override def underlyingPicklerSeq: Seq[Pickler[_]] = picklers
  }

  class InferredMetaKey1[E <: Storable, P1](
      name: String,
      ns: String,
      f: E => P1,
      uniq: Boolean,
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      val storableClass: Class[E],
      pickler: => Pickler[P1])
      extends InferredMetaKeyBase[E, P1](name, Seq(ns), f, uniq, indexed, default, isCollection) {

    override protected def pickleRep(res: P1, out: PickledOutputStream): Unit = {
      out.write(res, pickler)
    }

    override protected def stringForPrettyPrint(p: P1) = s"($p)"

    override def underlyingPicklerSeq: Seq[Pickler[_]] = Seq(pickler)
  }

  class InferredMetaKey0[E <: Storable](
      name: String,
      ns: Unit,
      f: E => Unit,
      uniq: Boolean,
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      val storableClass: Class[E])
      extends InferredMetaKeyBase[E, Unit](name, Nil, f, uniq, indexed, default, isCollection) {

    override protected def pickleRep(e: Unit, out: PickledOutputStream): Unit = ()

    override val underlyingPicklerSeq: Seq[Pickler[_]] = Nil
  }

  // Classes for non-unique indexes
  class InferredIndexInfo[E <: Storable, K <: Product](
      name: String,
      ns: Seq[String],
      f: E => K,
      picklers: => Seq[Pickler[_]],
      isCollection: Boolean,
      storableClass: Class[E])
      extends InferredMetaKey(name, ns, f, picklers, false, true, false, isCollection, storableClass)

  class InferredIndexInfo1[E <: Storable, A](
      name: String,
      ns: String,
      f: E => A,
      isCollection: Boolean,
      storableClass: Class[E],
      pickler: => Pickler[A])
      extends InferredMetaKey1(name, ns, f, false, true, false, isCollection, storableClass, pickler)

  trait IndexFindOps[A <: Entity, K] extends IndexFindImpl[A, K] {
    this: InferredMetaKeyBase[A, K] =>
    override final def queryable = true
  }

  trait IndexFindOpsWithErefFilter[A <: Entity, K] {
    this: InferredMetaKeyBase[A, K] =>
    override final def queryable = true
    override final def queryByEref = true
    def find(keyValue: K, entityIter: Iterable[A]): Iterable[A] =
      macro PluginSupport.findEntitiesWithErefFilterMacro[A, K]
  }

  trait IndexFindEventOps[A <: BusinessEvent, K] {
    this: InferredMetaKeyBase[A, K] =>
    override final def queryable = true
    @nodeSync def find(k: K): Iterable[A] = findEventsByIndexAtNow(makeKey(k))
    def find$queued(k: K): NodeFuture[Iterable[A]] = queuedNodeOf(findEventsByIndexAtNow(makeKey(k)))
  }

  trait EntityGetOps[A <: Entity, K] extends EntityGetImpl[A, K] {
    this: InferredMetaKeyBase[A, K] =>
    override final def queryable = true
    override def indexInfo: IndexInfo[A, K] = this
  }

  // Specialized for arity = 1
  def genEntityKeyInfo[E <: Entity, P1](
      name: String,
      prop: BaseIndexPropertyInfo[E, P1],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    prop.setKeyGen(
      new InferredMetaKey1(
        name,
        prop.name,
        prop.getValue _,
        true,
        indexed,
        default,
        isCollection,
        storableClass.asInstanceOf[Class[E]],
        prop.pickler))
  def genQueryableEntityKeyInfo[E <: Entity, P1](
      name: String,
      prop: BaseIndexPropertyInfo[E, P1],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    prop.setKeyGen(
      new InferredMetaKey1(
        name,
        prop.name,
        prop.getValue _,
        true,
        indexed,
        default,
        isCollection,
        storableClass.asInstanceOf[Class[E]],
        prop.pickler) with EntityGetOps[E, P1])
  def genEventKeyInfo[E <: BusinessEvent, P1](
      name: String,
      prop: BaseIndexPropertyInfo[E, P1],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    prop.setKeyGen(
      new InferredMetaKey1(
        name,
        prop.name,
        prop.getValue _,
        true,
        indexed,
        default,
        isCollection,
        storableClass.asInstanceOf[Class[E]],
        prop.pickler))
  def genIndexInfo[E <: Entity, P1](
      name: String,
      prop: BaseIndexPropertyInfo[E, P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    prop.setKeyGen(
      new InferredIndexInfo1(
        name,
        prop.name,
        prop.getValue _,
        isCollection,
        storableClass.asInstanceOf[Class[E]],
        prop.pickler))
  def genQueryableIndexInfo[E <: Entity, P1](
      name: String,
      prop: BaseIndexPropertyInfo[E, P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    prop.setKeyGen(
      new InferredIndexInfo1(
        name,
        prop.name,
        prop.getValue _,
        isCollection,
        storableClass.asInstanceOf[Class[E]],
        prop.pickler) with IndexFindOps[E, P1])
  def genQueryableIndexInfoWithErefFilter[E <: Entity, P1](
      name: String,
      prop: BaseIndexPropertyInfo[E, P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    prop.setKeyGen(
      new InferredIndexInfo1(
        name,
        prop.name,
        prop.getValue _,
        isCollection,
        storableClass.asInstanceOf[Class[E]],
        prop.pickler) with IndexFindOpsWithErefFilter[E, P1])
  def genEventIndexInfo[E <: BusinessEvent, P1](
      name: String,
      prop: BaseIndexPropertyInfo[E, P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    prop.setKeyGen(
      new InferredIndexInfo1(
        name,
        prop.name,
        prop.getValue _,
        isCollection,
        storableClass.asInstanceOf[Class[E]],
        prop.pickler))
  def genQueryableEventIndexInfo[E <: BusinessEvent, P1](
      name: String,
      prop: BaseIndexPropertyInfo[E, P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    prop.setKeyGen(
      new InferredIndexInfo1(
        name,
        prop.name,
        prop.getValue _,
        isCollection,
        storableClass.asInstanceOf[Class[E]],
        prop.pickler) with IndexFindEventOps[E, P1])

  // Supports: @stored @entity class SingleFieldDef(val name: String) { @key def theKey = name }
  def genQueryableEntityKeyInfo[E <: Entity, P1](
      name: String,
      prop: ReallyNontweakablePropertyInfo[E, P1],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredMetaKey1(
      name,
      prop.name,
      prop.getValue _,
      true,
      indexed,
      default,
      isCollection,
      storableClass.asInstanceOf[Class[E]],
      prop.pickler) with EntityGetOps[E, P1]
  // Supports: @stored @entity class SingleFieldDef(val name: String) { @indexed(queryable = false) def theKey = name }
  def genIndexInfo[E <: Entity, P1](
      name: String,
      prop: ReallyNontweakablePropertyInfo[E, P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo1(
      name,
      prop.name,
      prop.getValue _,
      isCollection,
      storableClass.asInstanceOf[Class[E]],
      prop.pickler)

  // Specialized for arity = 1
  def genEntityKeyInfo[E <: Entity, P1](
      name: String,
      ns: String,
      f: E => P1,
      pickler: => Pickler[P1],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredMetaKey1(
      name,
      ns,
      f,
      true,
      indexed,
      default,
      isCollection,
      storableClass.asInstanceOf[Class[E]],
      pickler)
  def genQueryableEntityKeyInfo[E <: Entity, P1](
      name: String,
      ns: String,
      f: E => P1,
      pickler: => Pickler[P1],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredMetaKey1(
      name,
      ns,
      f,
      true,
      indexed,
      default,
      isCollection,
      storableClass.asInstanceOf[Class[E]],
      pickler) with EntityGetOps[E, P1]
  def genEventKeyInfo[E <: BusinessEvent, P1](
      name: String,
      ns: String,
      f: E => P1,
      pickler: => Pickler[P1],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredMetaKey1(
      name,
      ns,
      f,
      true,
      indexed,
      default,
      isCollection,
      storableClass.asInstanceOf[Class[E]],
      pickler)
  def genIndexInfo[E <: Entity, P1](
      name: String,
      ns: String,
      f: E => P1,
      pickler: => Pickler[P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo1(name, ns, f, isCollection, storableClass.asInstanceOf[Class[E]], pickler)
  def genQueryableIndexInfo[E <: Entity, P1](
      name: String,
      ns: String,
      f: E => P1,
      pickler: => Pickler[P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo1(name, ns, f, isCollection, storableClass.asInstanceOf[Class[E]], pickler)
      with IndexFindOps[E, P1]
  def genQueryableIndexInfoWithErefFilter[E <: Entity, P1](
      name: String,
      ns: String,
      f: E => P1,
      pickler: => Pickler[P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo1(name, ns, f, isCollection, storableClass.asInstanceOf[Class[E]], pickler)
      with IndexFindOpsWithErefFilter[E, P1]
  def genEventIndexInfo[E <: BusinessEvent, P1](
      name: String,
      ns: String,
      f: E => P1,
      pickler: => Pickler[P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo1(name, ns, f, isCollection, storableClass.asInstanceOf[Class[E]], pickler)
  def genQueryableEventIndexInfo[E <: BusinessEvent, P1](
      name: String,
      ns: String,
      f: E => P1,
      pickler: => Pickler[P1],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo1(name, ns, f, isCollection, storableClass.asInstanceOf[Class[E]], pickler)
      with IndexFindEventOps[E, P1]

  // generic TupleN key generator
  def genEntityKeyInfo[E <: Entity, K <: Product](
      name: String,
      ns: Seq[String],
      f: E => K,
      picklers: => Seq[Pickler[_]],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredMetaKey(
      name,
      ns,
      f,
      picklers,
      true,
      indexed,
      default,
      isCollection,
      storableClass.asInstanceOf[Class[E]])
  def genQueryableEntityKeyInfo[E <: Entity, K <: Product](
      name: String,
      ns: Seq[String],
      f: E => K,
      picklers: => Seq[Pickler[_]],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredMetaKey(
      name,
      ns,
      f,
      picklers,
      true,
      indexed,
      default,
      isCollection,
      storableClass.asInstanceOf[Class[E]]) with EntityGetOps[E, K]
  def genEventKeyInfo[E <: BusinessEvent, K <: Product](
      name: String,
      ns: Seq[String],
      f: E => K,
      picklers: => Seq[Pickler[_]],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredMetaKey(
      name,
      ns,
      f,
      picklers,
      true,
      indexed,
      default,
      isCollection,
      storableClass.asInstanceOf[Class[E]])
  def genIndexInfo[E <: Entity, K <: Product](
      name: String,
      ns: Seq[String],
      f: E => K,
      picklers: => Seq[Pickler[_]],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo(name, ns, f, picklers, isCollection, storableClass.asInstanceOf[Class[E]])
  def genEventIndexInfo[E <: BusinessEvent, K <: Product](
      name: String,
      ns: Seq[String],
      f: E => K,
      picklers: => Seq[Pickler[_]],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo(name, ns, f, picklers, isCollection, storableClass.asInstanceOf[Class[E]])
  def genQueryableIndexInfo[E <: Entity, K <: Product](
      name: String,
      ns: Seq[String],
      f: E => K,
      picklers: => Seq[Pickler[_]],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo(name, ns, f, picklers, isCollection, storableClass.asInstanceOf[Class[E]])
      with IndexFindOps[E, K]
  def genQueryableIndexInfoWithErefFilter[E <: Entity, K <: Product](
      name: String,
      ns: Seq[String],
      f: E => K,
      picklers: => Seq[Pickler[_]],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo(name, ns, f, picklers, isCollection, storableClass.asInstanceOf[Class[E]])
      with IndexFindOpsWithErefFilter[E, K]
  def genQueryableEventIndexInfo[E <: BusinessEvent, K <: Product](
      name: String,
      ns: Seq[String],
      f: E => K,
      picklers: => Seq[Pickler[_]],
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredIndexInfo(name, ns, f, picklers, isCollection, storableClass.asInstanceOf[Class[E]])
      with IndexFindEventOps[E, K]

  def genEntityKeyInfo[E <: Entity](
      name: String,
      ns: Unit,
      f: E => Unit,
      picklersNotUsed: Seq[Pickler[_]],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredMetaKey0(name, ns, f, true, indexed, default, isCollection, storableClass.asInstanceOf[Class[E]])
  def genQueryableEntityKeyInfo[E <: Entity](
      name: String,
      ns: Unit,
      f: E => Unit,
      picklersNotUsed: Seq[Pickler[_]],
      indexed: Boolean,
      default: Boolean,
      isCollection: Boolean,
      storableClass: Class[_] /* actually E but that causes type inference problems */ ) =
    new InferredMetaKey0(name, ns, f, true, indexed, default, isCollection, storableClass.asInstanceOf[Class[E]])
      with EntityGetOps[E, Unit]

  def genProjectedMember[E, T](name: String, indexed: Boolean, queryable: Boolean, f: E => T): MemberDescriptor =
    macro PluginSupport.genProjectedMemberMacro[E, T]

  def genReifiedLambda[T](f: T): LambdaElement = macro PluginSupport.genReifiedLambdaMacro[T]

  @node final def getEntityByKeyAtNow[E <: Entity](key: Key[E]): E = {
    DAL.resolver.findEntity(key, loadContext)
  }

  @node final def getEntityByKeyAtNowOption[E <: Entity](key: Key[E]): Option[E] = {
    DAL.resolver.findEntityOption(key, loadContext)
  }

  @node final def getEventByKeyAtNow[E <: BusinessEvent](key: Key[E]): E = {
    DAL.resolver.findEvent(key, loadContext)
  }

  @node final def getEventByKeyAtNowOption[E <: BusinessEvent](key: Key[E]): Option[E] = {
    DAL.resolver.findEventOption(key, loadContext)
  }

  @node final def findEntitiesByIndexAtNow[E <: Entity](key: Key[E]) = {
    DAL.resolver.findByIndex(key, loadContext)
  }

  @node final def findEntitiesInRangeAtNow[E <: Entity](key: Key[E], fromTemporalContext: TemporalContext) = {
    DAL.resolver.findByIndexInRange(key, fromTemporalContext, loadContext)
  }

  @node final def findEntitiesByIndexWithErefAtNow[E <: Entity](key: Key[E], erefs: Iterable[EntityReference]) = {
    DAL.resolver.findByIndexWithEref(key, erefs, loadContext)
  }

  @node final def findEventsByIndexAtNow[E <: BusinessEvent](key: Key[E]) = {
    DAL.resolver.findEventsByIndex(key, loadContext)
  }

  final def disabledConstructorDefaultValues(name: String) =
    throw new IncompatibleVersionException(
      s"Compiler-generated $$init methods are disabled and the property \'$name\' was not found in the pickled stream. You should explicitly provide a \'$name$$init\' method. As a temporary workaround, you can re-enable the compiler-generated method by running with -D${optimus.versioning.RuntimeConstants.generatedInitMethodsProperty}=true, though this behaviour is not future-proof.")
  final def missingProperty(name: String): Unit = ReflectiveEntityPicklingImpl.missingProperty(name)

  private[this] def handleNode(node: PropertyNode[_]): Seq[Entity] = node.result match {
    case entity: Entity  => Seq(entity)
    case Some(e: Entity) => Seq(e)
    case None            => Nil
    case entities_? : Iterable[_] =>
      entities_?.headOption match {
        case Some(firstEntity: Entity) =>
          // hopefully this means they're all entities...
          entities_?.asInstanceOf[Iterable[Entity]].toSeq
        case _ => Nil
      }
    case _ =>
      // @stored @entity class Foo(@node val blargh: String)
      // no entity references here, just an ACPN for the value of `blargh`
      Nil
  }
  import optimus.platform.storable.EntityReference
  final def getEntityReference(pnode: NodeKey[_]): Seq[EntityReference] = {
    pnode match {
      case node: MaybePickledReference[_]         => getEntityReferenceFromPickledRepr(node, node.pickled)
      case cnode: AlreadyCompletedPropertyNode[_] => handleNode(cnode).map(_.dal$entityRef)
      // illegal cases
      case badNode: PropertyNode[_] =>
        throw new IllegalStateException(s"Unexpected node type $badNode ${badNode.getClass}")
    }
  }

  private def getEntityReferenceFromPickledRepr(node: PropertyNode[_], pickledOrNull: Any): Seq[EntityReference] = {
    pickledOrNull match {
      case eref: EntityReference => eref :: Nil
      case _: ModuleEntityToken =>
        null :: Nil // avoid class loading, m.readResolve.asInstanceOf[Entity].dal$entityRef :: Nil
      case Some(eref: EntityReference)                 => eref :: Nil
      case None                                        => Nil
      case erefs: Iterable[EntityReference @unchecked] => erefs.toSeq
      case null => // already unpickled (after lazy load)
        handleNode(node).map(_.dal$entityRef)
    }
  }
  final def getEntityReferenceHolders[T <: Entity](node: NodeKey[_]): Iterable[EntityReferenceHolder[T]] = {
    def fromHydratedNode(n: PropertyNode[_]): Iterable[EntityReferenceHolder[T]] =
      handleNode(n).map(hydratedEntity => EntityReferenceHolder(hydratedEntity.asInstanceOf[T]))

    node match {
      case l: MaybePickledReference[_] =>
        l.pickled match {
          case eref: EntityReference => Iterable(EntityReferenceHolder(eref, l.entity.dal$temporalContext, None, None))
          case erefs: Iterable[EntityReference] @unchecked =>
            erefs.map(eref => EntityReferenceHolder[T](eref, l.entity.dal$temporalContext, None, None))
          case null => fromHydratedNode(l) // Already unpickled..
          case unexpected =>
            throw new IllegalStateException(
              s"Cannot create reference holder from pickled value of type: ${unexpected.getClass.getName}.")
        }
      // Heap entity(s).
      case cnode: AlreadyCompletedPropertyNode[_] => fromHydratedNode(cnode)
      case badNode: PropertyNode[_] =>
        throw new IllegalStateException(s"Unexpected node type ${badNode.getClass}")
    }
  }
}

class BaseIndexPropertyInfo[E <: Storable, R](name: String, flags: Long, annotations: Seq[StaticAnnotation] = Nil)
    extends ReallyNontweakablePropertyInfo[E, R](name, flags, annotations) {
  private[this] var keyGen: PluginSupport.InferredMetaKeyBase[E, R] = _
  private[graph] def setKeyGen[T <: PluginSupport.InferredMetaKeyBase[E, R]](kg: T) = {
    keyGen = kg
    kg
  }
  def makeKey(key: PropType): Key[E] = keyGen.makeKey(key)
  final def indexInfo: IndexInfo[E, R] = keyGen
}

object IndexPropertyInfoMacro {
  class EntitledOnlyParam(value: Boolean) extends StaticAnnotation

  def findImpl[E <: Entity, R: c.WeakTypeTag](c: Context)(key: c.Expr[Any]): c.Expr[Iterable[E]] = {
    import c.universe._
    val paramType = c.weakTypeOf[R]
    val caller = c.prefix.asInstanceOf[c.Expr[IndexFindImpl[E, R]]]
    val entitledOnlyParam = c.macroApplication.symbol.annotations
      .find(_.tree.tpe <:< typeOf[this.EntitledOnlyParam])
      .flatMap(_.tree.children.tail.collectFirst({ case Literal(Constant(b: Boolean)) => b }))
      .getOrElse(false)
    val exprEntitleOnlyParam = c.Expr[Boolean](Literal(Constant(entitledOnlyParam)))

    def reportTypeError(expect: Any, actual: Any): c.Expr[Iterable[E]] = {
      OptimusReporter.alarm(c, DALAlarms.MISMATCHED_ENTITY_INDEX_TYPE)(key.tree.pos, expect, actual)
      c.Expr[Iterable[E]](EmptyTree)
    }

    if (key.tree.tpe <:< paramType) {
      reify(
        caller.splice.$findByPropType(c.Expr[R](key.tree).splice, exprEntitleOnlyParam.splice)
      ) // this will go back to normal call
    } else if (paramType <:< c.weakTypeOf[Set[_]] && paramType.typeArgs.size == 1) {
      if (key.tree.tpe <:< paramType.typeArgs.head)
        reify(caller.splice.$findByPropType(Set.apply(key.splice).asInstanceOf[R], exprEntitleOnlyParam.splice))
      else
        reportTypeError(paramType.typeArgs.head, key.actualType)
    } else if (paramType <:< c.weakTypeOf[Traversable[_]] && paramType.typeArgs.size == 1) {
      if (key.tree.tpe <:< paramType.typeArgs.head)
        reify(
          caller.splice.$findByPropType(List.apply(key.splice).asInstanceOf[R], exprEntitleOnlyParam.splice)
        ) // we can define a new findCollection method and do special logic there
      else
        reportTypeError(paramType.typeArgs.head, key.actualType)
    } else {
      reportTypeError(paramType, key.actualType)
    }
  }
}

trait IndexFindImpl[E <: Entity, R] {
  def makeKey(key: R): Key[E]

  @IndexPropertyInfoMacro.EntitledOnlyParam(false)
  def find(key: Any): Iterable[E] = macro IndexPropertyInfoMacro.findImpl[E, R]
  @IndexPropertyInfoMacro.EntitledOnlyParam(true)
  def findEntitledOnly(key: Any): Iterable[E] = macro IndexPropertyInfoMacro.findImpl[E, R]

  @nodeSync def $findByPropType(key: R, entitledOnly: Boolean): Iterable[E] =
    DAL.resolver.findByIndex(makeKey(key), loadContext, None, entitledOnly)
  def $findByPropType$queued(key: R, entitledOnly: Boolean): NodeFuture[Iterable[E]] =
    queuedNodeOf(DAL.resolver.findByIndex(makeKey(key), loadContext, None, entitledOnly))

  @nodeSync
  def findInRange(key: R, fromTemporalContext: TemporalContext): Iterable[VersionHolder[E]] =
    DAL.resolver.findByIndexInRange(makeKey(key), fromTemporalContext, loadContext)
  def findInRange$queued(key: R, fromTemporalContext: TemporalContext): NodeFuture[Iterable[VersionHolder[E]]] =
    queuedNodeOf(DAL.resolver.findByIndexInRange(makeKey(key), fromTemporalContext, loadContext))
}

// These are generated for non-@node @index val entity properties.
final class IndexPropertyInfo[E <: Entity, R](name: String, flags: Long, annotations: Seq[StaticAnnotation] = Nil)
    extends BaseIndexPropertyInfo[E, R](name, flags, annotations)
    with IndexFindImpl[E, R]

final class IndexPropertyInfoForErefFilter[E <: Entity, R](
    name: String,
    flags: Long,
    annotations: Seq[StaticAnnotation] = Nil)
    extends BaseIndexPropertyInfo[E, R](name, flags, annotations) {
  def find(keyValue: PropType, entityIter: Iterable[E]): Iterable[E] =
    macro PluginSupport
      .findEntitiesWithErefFilterMacro[E, R] // aka PluginSupport.findEntitiesByIndexAtNow(new zz.makeKey(zz));
}

trait EntityGetImpl[E <: Entity, R] {
  import PluginSupport._

  def makeKey(key: R): Key[E]
  def indexInfo: IndexInfo[E, R]

  @nodeSync def get(k: R): E = getEntityByKeyAtNow(makeKey(k))
  def get$queued(k: R): NodeFuture[E] = queuedNodeOf(getEntityByKeyAtNow(makeKey(k)))

  @nodeSync def getOption(k: R): Option[E] = getEntityByKeyAtNowOption(makeKey(k))
  def getOption$queued(k: R): NodeFuture[Option[E]] = queuedNodeOf(getEntityByKeyAtNowOption(makeKey(k)))

  @node def enumerate(): Iterable[SortedPropertyValues] = {
    val resolver = EvaluationContext.entityResolver.asInstanceOf[EntityResolverReadImpl]
    val dsiQt =
      QueryTemporalityFinder.findQueryTemporality(
        loadContext,
        QueryByClass(resolver.dataAccess, indexInfo.storableClass))
    val qt = QueryTemporality.At(dsiQt.validTime, dsiQt.txTime)
    resolver.enumerateKeysWithRtt(indexInfo, qt, qt.txTime)
  }

  @node def enumerate(when: QueryTemporality): Iterable[SortedPropertyValues] = {
    val resolver = EvaluationContext.entityResolver.asInstanceOf[EntityResolverReadImpl]
    resolver.enumerateKeys(indexInfo, when)
  }
}

final class UniqueIndexPropertyInfo[E <: Entity, R](name: String, flags: Long, annotations: Seq[StaticAnnotation] = Nil)
    extends BaseIndexPropertyInfo[E, R](name, flags, annotations)
    with EntityGetImpl[E, R]

object EventIndexPropertyInfoMacro {
  class EntitledOnlyParam(value: Boolean) extends StaticAnnotation

  def findImpl[E <: BusinessEvent, R: c.WeakTypeTag](c: Context)(key: c.Expr[Any]): c.Expr[Iterable[E]] = {
    import c.universe._
    val paramType = c.weakTypeOf[R]
    val caller = c.prefix.asInstanceOf[c.Expr[EventIndexPropertyInfo[E, R]]]
    val entitledOnlyParam = c.macroApplication.symbol.annotations
      .find(_.tree.tpe <:< typeOf[this.EntitledOnlyParam])
      .flatMap(_.tree.children.tail.collectFirst({ case Literal(Constant(b: Boolean)) => b }))
      .getOrElse(false)
    val exprEntitleOnlyParam = c.Expr[Boolean](Literal(Constant(entitledOnlyParam)))

    if (key.tree.tpe <:< paramType)
      reify(
        caller.splice.$findByPropType(c.Expr[R](key.tree).splice, exprEntitleOnlyParam.splice)
      ) // this will go back to normal call
    else if (
      paramType <:< c
        .weakTypeOf[Set[_]] && (paramType.typeArgs.size == 1 && key.tree.tpe <:< paramType.typeArgs.head)
    )
      reify(
        caller.splice.$findByPropType(Set.apply(key.splice).asInstanceOf[R], exprEntitleOnlyParam.splice)
      ) // we can define a new findCollection method and do special logic there
    else if (
      paramType <:< c
        .weakTypeOf[Traversable[_]] && (paramType.typeArgs.size == 1 && key.tree.tpe <:< paramType.typeArgs.head)
    )
      reify(
        caller.splice.$findByPropType(List.apply(key.splice).asInstanceOf[R], exprEntitleOnlyParam.splice)
      ) // we can define a new findCollection method and do special logic there
    else {
      OptimusReporter.alarm(c, DALAlarms.MISMATCHED_EVENT_INDEX_TYPE)(key.tree.pos, paramType, key.actualType)
      c.Expr[Iterable[E]](EmptyTree)
    }
  }
}

final class EventIndexPropertyInfo[E <: BusinessEvent, R](
    name: String,
    flags: Long,
    annotations: Seq[StaticAnnotation] = Nil)
    extends BaseIndexPropertyInfo[E, R](name, flags, annotations) {
  @EventIndexPropertyInfoMacro.EntitledOnlyParam(false)
  def find(key: Any): Iterable[E] = macro EventIndexPropertyInfoMacro.findImpl[E, R]
  @EventIndexPropertyInfoMacro.EntitledOnlyParam(true)
  def findEntitledOnly(key: Any): Iterable[E] = macro EventIndexPropertyInfoMacro.findImpl[E, R]

  @nodeSync def $findByPropType(key: PropType, entitledOnly: Boolean): Iterable[E] =
    DAL.resolver.findEventsByIndex(makeKey(key), loadContext, entitledOnly)
  def $findByPropType$queued(key: PropType, entitledOnly: Boolean): NodeFuture[Iterable[E]] =
    queuedNodeOf(DAL.resolver.findEventsByIndex(makeKey(key), loadContext, entitledOnly))
}

final class EventUniqueIndexPropertyInfo[E <: BusinessEvent, R](
    name: String,
    flags: Long,
    annotations: Seq[StaticAnnotation] = Nil)
    extends BaseIndexPropertyInfo[E, R](name, flags, annotations) {
  @nodeSync
  def get(key: PropType): E = PluginSupport.getEventByKeyAtNow[E](makeKey(key))
  def get$queued(key: PropType): NodeFuture[E] = queuedNodeOf(PluginSupport.getEventByKeyAtNow[E](makeKey(key)))
  @nodeSync def getOption(key: PropType): Option[E] = PluginSupport.getEventByKeyAtNowOption[E](makeKey(key))
  def getOption$queued(key: PropType): NodeFuture[Option[E]] =
    queuedNodeOf(PluginSupport.getEventByKeyAtNowOption[E](makeKey(key)))
}

final class Child2ParentReallyNontweakablePropertyInfo[E <: Entity, R](
    name: String,
    flags: Long,
    annotations: Seq[StaticAnnotation] = Nil)
    extends ReallyNontweakablePropertyInfo[E, R](name, flags, annotations) {

  @nodeSync
  def find(lookupSrc: Entity, temporalContext: TemporalContext): Iterable[E] =
    resolver.getLinkedEntities[E](lookupSrc, name, temporalContext)(entityClassTag)
  def find$queued(lookupSrc: Entity, temporalContext: TemporalContext): NodeFuture[Iterable[E]] =
    queuedNodeOf(resolver.getLinkedEntities[E](lookupSrc, name, temporalContext)(entityClassTag))

  @nodeSync
  def find(lookupSrc: Entity): Iterable[E] = resolver.getLinkedEntities2[E](lookupSrc, name)(entityClassTag)
  def find$queued(lookupSrc: Entity): NodeFuture[Iterable[E]] = queuedNodeOf(
    resolver.getLinkedEntities2[E](lookupSrc, name)(entityClassTag))

  override def isChildToParent: Boolean = true
}

final class Child2ParentPropertyInfo0[E <: Entity, R](
    name: String,
    flags: Long,
    annotations: Seq[StaticAnnotation] = Nil)
    extends ReallyNontweakablePropertyInfo[E, R](name, flags, annotations) {

  @nodeSync
  def find(lookupSrc: Entity, temporalContext: TemporalContext): Iterable[E] =
    resolver.getLinkedEntities[E](lookupSrc, name, temporalContext)(entityClassTag)
  def find$queued(lookupSrc: Entity, temporalContext: TemporalContext): NodeFuture[Iterable[E]] =
    queuedNodeOf(resolver.getLinkedEntities[E](lookupSrc, name, temporalContext)(entityClassTag))

  @nodeSync
  def find(lookupSrc: Entity): Iterable[E] = resolver.getLinkedEntities2[E](lookupSrc, name)(entityClassTag)
  def find$queued(lookupSrc: Entity): NodeFuture[Iterable[E]] =
    queuedNodeOf(resolver.getLinkedEntities2[E](lookupSrc, name)(entityClassTag))

  override def isChildToParent: Boolean = true
}

final class Child2ParentTwkPropertyInfo0[E <: Entity, R](
    name: String,
    flags: Long,
    annotations: Seq[StaticAnnotation] = Nil)
    extends PropertyInfo0[E, R](name, flags, annotations) {

  @nodeSync
  def find(lookupSrc: Entity, temporalContext: TemporalContext): Iterable[E] =
    resolver.getLinkedEntities[E](lookupSrc, name, temporalContext)(entityClassTag)
  def find$queued(lookupSrc: Entity, temporalContext: TemporalContext): NodeFuture[Iterable[E]] =
    queuedNodeOf(resolver.getLinkedEntities[E](lookupSrc, name, temporalContext)(entityClassTag))

  @nodeSync
  def find(lookupSrc: Entity): Iterable[E] = resolver.getLinkedEntities2[E](lookupSrc, name)(entityClassTag)
  def find$queued(lookupSrc: Entity): NodeFuture[Iterable[E]] = queuedNodeOf(
    resolver.getLinkedEntities2[E](lookupSrc, name)(entityClassTag))

  override def isChildToParent: Boolean = true
}

class GenProjectedMemberMacros[C <: Context](val c: C) extends VersioningMacroUtils[C] {
  import c.universe._

  private val PicklingMacroUtils = new PicklingMacroUtils[c.type](c)

  def genProjectedMember[E: c.WeakTypeTag, T: c.WeakTypeTag](
      name: c.Expr[String],
      indexed: c.Expr[Boolean],
      queryable: c.Expr[Boolean],
      f: c.Expr[E => T]): c.Expr[MemberDescriptor] = {
    val ft = c.Expr[RegisteredFieldType](trees.registeredFieldType(weakTypeOf[T].fieldType))
    val T = c.weakTypeOf[T]
    val unpickler = c.Expr[Unpickler[T]] {
      PicklingMacroUtils.selectRegistryUnpicklerMethod(T)
    }
    val typeInfo = c.Expr[TypeInfo[T]](q"optimus.platform.relational.tree.typeInfo[$T]")
    reify {
      // we are not interested in the reflect type, we only generate this for
      // '@projected def' that references embeddable attribute path
      new RuntimeFieldDescriptor(
        TypeInfo.noInfo[E],
        name.splice,
        typeInfo.splice,
        Map(
          "fieldType" -> ft.splice,
          "unpickler" -> unpickler.splice,
          "indexed" -> indexed.splice,
          "queryable" -> queryable.splice)
      ).asInstanceOf[MemberDescriptor]
    }
  }
}
