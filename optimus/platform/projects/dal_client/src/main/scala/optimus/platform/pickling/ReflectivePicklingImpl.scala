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
package optimus.platform.pickling

import optimus.core.CoreHelpers
import optimus.entity.EntityInfo
import optimus.entity.StorableInfo
import optimus.graph.AlreadyCompletedPropertyNode
import optimus.graph._
import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform.dal.DALEventInfo
import optimus.platform.dal.IncompatibleVersionException
import optimus.platform.pickling.UnsafeFieldInfo.StorageKind
import optimus.platform.storable._
import optimus.platform.util.Log
import sun.misc.Unsafe

import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType.methodType
import java.lang.reflect.Field
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object ReflectivePicklingImpl {
  private val setterMType = methodType(classOf[Unit], classOf[Storable], classOf[AnyRef])
  private val getterMType = methodType(classOf[AnyRef], classOf[Storable])
  protected[pickling] val unsafe: Unsafe = UnsafeAccess.getUnsafe

  def writePropertyValue(inst: Storable, fieldInfo: UnsafeFieldInfo, out: PickledOutputStream): Unit = {
    val pickler = fieldInfo.pinfo.pickler.asInstanceOf[Pickler[AnyRef]]

    @entersGraph def writeNode(pn: PropertyNode[AnyRef]): Unit = {
      EvaluationContext.lookupNode(pn) match {
        case lpr: MaybePickledReference[AnyRef @unchecked] if !out.forceHydration =>
          // this is a LazyPickledReference (or one of its internal proxies) so can be written raw without
          // needing to actually run it (so potentially avoiding an unpickle and associated DAL fetch), unless it
          // had already been unpickled (in which case write it normally)
          val p = lpr.pickled
          if (p != null) {
            // During unpickling an entity, we would have wrapped any StorageKind.Node field that is of a collection
            // type in an UnresolvedItemWrapper for PluginHelpers.resolveEntity* methods to handle their
            // lazy unpickling. If the actual unpickling did not yet happen, we need to unwrap here to get back
            // the original, raw pickled-form. The need to use this UnresolvedItemWrapper is explained
            // in def unpickleFill below
            val raw = p match {
              case v: UnresolvedItemWrapper[_] => v.unresolved
              case _                           => p
            }
            out.writeRawObject(raw.asInstanceOf[AnyRef])
          } else {
            out.write(lpr.await, pickler)
          }
        case node =>
          out.write(node.await, pickler)
      }
    }

    val fValue = fieldInfo.fieldReader.invokeExact(inst: Storable): AnyRef
    fieldInfo.storageKind match {
      case UnsafeFieldInfo.StorageKind.Node | UnsafeFieldInfo.StorageKind.LazyPickled |
          UnsafeFieldInfo.StorageKind.Tweakable =>
        writeNode(fValue.asInstanceOf[PropertyNode[AnyRef]])
      case UnsafeFieldInfo.StorageKind.Val =>
        out.write(fValue, pickler)
    }
  }
}
import optimus.platform.pickling.ReflectivePicklingImpl._

/**
 * Base functionality for pickling and unpickling.
 *
 * @tparam Info
 *   The type of descriptors
 * @tparam InitInfo
 * @tparam Reference
 *   The type of references
 * @tparam Unpickled
 *   The unpickled base type
 */
abstract class ReflectivePicklingImpl[Info <: StorableInfo, InitInfo, Reference, Unpickled <: Storable]
    extends ReflectionUtils
    with Log {
  protected def initMetadata(
      inst: Unpickled,
      is: PickledInputStream,
      initInfo: InitInfo,
      info: Info,
      ref: Reference): Unit
  protected[optimus] def missingProperty(name: String): Nothing

  def pickle(inst: Unpickled, out: PickledOutputStream): Unit = {
    inst.$info.unsafeFieldInfo.asyncOff.foreach { fieldInfo =>
      out.writePropertyInfo(fieldInfo.pinfo)
      writePropertyValue(inst, fieldInfo, out)
    }
  }

  /**
   * Create an Entity or Event instance wholesale from an unpickling stream.
   *
   * @param info
   *   Entity descriptor.
   * @param is
   *   The input stream from which to read data.
   * @param forceUnpickle
   *   Whether to eagerly or lazily unpickle things
   * @param initInfo
   * @param ref
   *   Reference into which to put the created Entity
   * @return
   *   The new Entity or Event instance
   */
  def unpickleCreate(
      info: Info,
      is: PickledInputStream,
      forceUnpickle: Boolean,
      initInfo: InitInfo,
      ref: Reference): Unpickled = {
    val inst = unsafe.allocateInstance(info.runtimeClass).asInstanceOf[Unpickled]
    initMetadata(inst, is, initInfo, info, ref)
    unpickleFill(info, is, forceUnpickle, incomplete = false, inst)
    inst
  }

  /**
   * Fill in fields in an instance from an unpickling stream. This can require all fields to be present, as in the case
   * of Entity creation, or it can only fill in some, as is the case with splats.
   *
   * @param info
   *   Entity descriptor
   * @param is
   *   Input stream from which to read the data.
   * @param forceUnpickle
   *   Whether or not to unpickle eagerly
   * @param incomplete
   *   Whether to expect all fields, or a partial.
   * @param inst
   *   The instance to fill
   */
  private def unpickleFill(
      info: StorableInfo,
      is: PickledInputStream,
      forceUnpickle: Boolean,
      incomplete: Boolean,
      inst: Unpickled): Unit = {
    info.unsafeFieldInfo
      // TODO (OPTIMUS-22294): Asyncing the closure triggers sync stack detection.  Apparently sync stacks are ok.
      .asyncOff
      .foreach { case UnsafeFieldInfo(pinfo, storageKind, initMethod, fieldReader, fieldSetter) =>
        if (storageKind == UnsafeFieldInfo.StorageKind.Node) {
          // handle Entity/Option[Entity]/Collection[Entity] cases for traits. For now, we're still
          // using the old-style LPR approach for traits
          val hasValue = if (forceUnpickle) is.seek(pinfo.name, pinfo.unpickler) else is.seekRaw(pinfo.name)
          if (hasValue || !incomplete) {
            val value: PropertyNode[_] = {
              if (hasValue) {
                if (forceUnpickle) new AlreadyCompletedPropertyNode(is.value, inst.asInstanceOf[Entity], pinfo)
                else
                  new LazyPickledReference(
                    is.value,
                    pinfo.unpickler.asInstanceOf[Unpickler[AnyRef]],
                    inst.asInstanceOf[Entity],
                    pinfo,
                    null)
              } else {
                if (initMethod.isDefined)
                  new AlreadyCompletedPropertyNode(initMethod.get.invoke(inst), inst.asInstanceOf[Entity], pinfo)
                else missingProperty(pinfo.name)
              }
            }

            fieldSetter.invokeExact(inst, value: AnyRef): Unit // Type ascription is not optional
          }
        } else if (storageKind == UnsafeFieldInfo.StorageKind.LazyPickled) { // handle Entity/Option[Entity]/Collection[Entity] cases
          val hasValue = if (forceUnpickle) is.seek(pinfo.name, pinfo.unpickler) else is.seekRaw(pinfo.name)
          if (hasValue || !incomplete) {
            val value = {
              if (hasValue) {
                if (forceUnpickle) {
                  is.value
                } else {
                  // Field is a complex type that contains an @entity. This could be a collection, tuple, embeddable
                  // or some other type that has a custom pickler for.
                  // While we should not need LazyPickled for vals that reference case objects, if the case object is
                  // nested inside of an @entity object, the plugin marks it as Lazy. This logic in the plugin can be
                  // improved by not considering the containing class when looking for reachable entities.
                  //
                  // The actual lazy unpickling will be done by the PluginHelpers.resolve* methods as long as
                  // the type we set into the field extends ContainsUnresolvedReference.
                  is.value match {
                    case knownUnresolved: ContainsUnresolvedReference =>
                      // In this case, the pickled representation implements ContainsUnresolvedReference so it is
                      // suitable to just return.
                      knownUnresolved
                    case toWrap: AnyRef =>
                      // Needs to be wrapped in an UnresolvedItemWrapper as we need something that
                      // implements ContainsUnresolvedReference
                      new UnresolvedItemWrapper(toWrap)
                    case _ =>
                      // We don't expect is.value to be scala.AnyVal and still be marked as LazyPickled!
                      throw new UnpickleException(is.value, "AnyRef")
                  }
                }
              } else {
                if (initMethod.isDefined)
                  initMethod.get.invoke(inst)
                else missingProperty(pinfo.name)
              }
            }

            fieldSetter.invokeExact(inst, value: Any): Unit // Type ascriptions are not optional
          }
        } else { // handle non-entity types - primary types, embeddable, Option/Collection of such types
          val hasValue = is.seek(pinfo.name, pinfo.unpickler)
          if (hasValue || !incomplete) {
            val value = {
              if (hasValue) is.value
              else if (initMethod.isDefined) initMethod.get.invoke(inst)
              else if (!incomplete) missingProperty(pinfo.name)
            }

            try {
              fieldSetter.invokeExact(inst, value): Unit // Type ascription is not optional
            } catch {
              case NonFatal(e) =>
                throw new IncompatibleVersionException(
                  s"Failed to unpickle field $pinfo (typeTag: ${pinfo.typeTag}) with value '$value'",
                  e)
            }
          }
        }
      }
  }

  protected def resolveSetter(lookup: MethodHandles.Lookup, fld: Field, isVal: Boolean): MethodHandle = {
    val setter = lookup.unreflectSetter(fld).asType(setterMType)
    val intern = isVal && !fld.getType.isPrimitive && fld.getAnnotation(classOf[_intern]) != null
    if (intern)
      MethodHandles.filterArguments(setter, 1, CoreHelpers.intern_mh)
    else
      setter
  }

  protected def resolveReader(lookup: MethodHandles.Lookup, pinfo: PropertyInfo[_], sk: StorageKind): MethodHandle = {
    val getterName = if (sk == StorageKind.Val) pinfo.name else pinfo.name + "$newNode"
    val getter = lookupMethod(pinfo.entityInfo.runtimeClass, getterName)
    lookup.unreflect(getter).asType(getterMType)
  }

  // TODO (OPTIMUS-24163): This can, should, and will be a compile error with this number.
  protected final def propertyInfoNotFoundError(which: String, where: Class[_]): Unit = {
    log.error(
      s"Can't find property $which in $where. Deserializing such an object will doubtlessly disappoint. (12000)")
  }

  private def warnOnDangerousReactiveUpdate(
      inst: Unpickled,
      pinfo: PropertyInfo[_],
      reader: MethodHandle,
      current: Any): Unit = {
    val info =
      try {
        val prev = reader.invokeExact(inst): AnyRef
        s"$inst.${pinfo.name}: $prev => $current"
      } catch { case t: Throwable => s"<error: ${t.getMessage}>" }
    log.warn("Dangerous Reactive Update for " + info)
  }
}

object ReflectiveEntityPicklingImpl // used in [core]o.p.pickling.ReflectiveEntityPickling.instance
    extends ReflectivePicklingImpl[EntityInfo, StorageInfo, EntityReference, Entity]
    with ReflectiveEntityPickling {
  override protected[optimus] def missingProperty(name: String): Nothing =
    throw new IncompatibleVersionException(
      s"Cannot deserialize entity since the property \'$name\' was not found in the pickled stream. In order to facilitate data migration define a versioning transformer. Please check codetree docs for more details on DAL versioning.")

  protected override def initMetadata(
      inst: Entity,
      is: PickledInputStream,
      initInfo: StorageInfo,
      info: EntityInfo,
      ref: EntityReference): Unit = {
    EntityInternals.initEntityFlavor(inst, EntityFlavor(is, initInfo, ref))
  }

  private def getAllParents(info: EntityInfo): Seq[EntityInfo] = {
    info +: (info.parents flatMap { p =>
      getAllParents(p)
    })
  }

  def prepareMeta(info: EntityInfo): Seq[UnsafeFieldInfo] = {
    val allInfos = getAllParents(info)
    val props = allInfos.flatMap { _.runtimeClass.getDeclaredFields }
    props flatMap { p =>
      val name = p.getName
      val endIndex = Math.max(name.lastIndexOf("$impl"), name.lastIndexOf("$backing"))
      val beginIndex = {
        // handle the places we put package name as part of the property name to avoid name conflict
        val res = name.lastIndexOf("$$")
        if (res > 0) res + 2 else 0
      }
      if (endIndex > 0) {
        val pname = name.substring(beginIndex, endIndex)
        val initMethod = info.runtimeClass.getMethods.find(m => m.getName == pname + "$init")
        val pinfo: Option[PropertyInfo[_]] = {
          val pinfos = allInfos.collect {
            case i if i.propertyMetadata.contains(pname) => i.propertyMetadata(pname)
          }

          if (pinfos.nonEmpty) {
            // search for the stored property first
            val storedPinfo = pinfos.find(_.isStored)
            if (storedPinfo.isDefined) Some(storedPinfo.get)
            else if (initMethod.isDefined) {
              // if we can't find a stored property in the whole inheritance chain, (e.g. @transient val)
              // using the first found property with $init method as workaround
              Some(pinfos.head) // could be headOption, but this shows our certainty that it's here
            } else {
              // we can't find a way to initialize the transient val property, throw
              throw new IllegalStateException(
                s"Can't find storeable property $pname or init method in ${info.runtimeClass.getName}")
            }
          } else {
            // we don't find any property info named as such value, (e.g. val definition in non-entity trait)
            // we don't have a way to get the initialization logic, throw
            // (well, think very hard about throwing and wish we could. the day will come; oh yes, it will come.)
            propertyInfoNotFoundError(pname, info.runtimeClass)
            None
          }
        }

        pinfo map { pinfo =>
          val lookup = MethodHandles.lookup()
          p.setAccessible(true)

          val nodeKind =
            if ((classOf[Node[_]] isAssignableFrom p.getType))
              StorageKind.Node
            else if (java.lang.reflect.Modifier.isVolatile(p.getModifiers)) {
              // implying that this $impl field is for an lazypickled value from the volatile attribute
              // We will improve this by using a PropertyInfo flag instead.
              StorageKind.LazyPickled
            } else if (pinfo.isTweakable) StorageKind.Tweakable
            else StorageKind.Val

          UnsafeFieldInfo(
            pinfo,
            nodeKind,
            initMethod,
            resolveReader(lookup, pinfo, nodeKind),
            resolveSetter(lookup, p, nodeKind == StorageKind.Val)
          )
        }
      } else None
    }
  }

}

object ReflectiveEventPickling
    extends ReflectivePicklingImpl[EventInfo, DALEventInfo, BusinessEventReference, BusinessEvent] {
  override protected[optimus] def missingProperty(name: String): Nothing =
    throw new IncompatibleVersionException(
      s"Cannot deserialize event since the property \'$name\' was not found in the pickled stream. In order to facilitate data migration define a versioning transformer. Please check codetree docs for more details on DAL versioning.")

  protected override def initMetadata(
      inst: BusinessEvent,
      is: PickledInputStream,
      initInfo: DALEventInfo,
      info: EventInfo,
      ref: BusinessEventReference): Unit = {
    inst.asInstanceOf[BusinessEventImpl].initEvent(initInfo, ref)
    info.validTime_mh.invokeExact(inst, is.temporalContext.unsafeValidTime): Unit // Type ascription not optional
  }

  private def getAllParents(info: EventInfo): Seq[EventInfo] = {
    info +: (info.parents flatMap { p =>
      getAllParents(p)
    })
  }

  def prepareMeta(info: EventInfo): Seq[UnsafeFieldInfo] = {
    val allInfos = getAllParents(info)
    val props = allInfos.flatMap { _.runtimeClass.getDeclaredFields }
    props filter (_.getName != "validTime") flatMap { p =>
      val name = p.getName
      val beginIndex = {
        // handle the places we put package name as part of the property name to avoid name conflict
        val res = name.lastIndexOf("$$")
        if (res > 0) res + 2 else 0
      }
      val pname = (if (beginIndex > 0) name.substring(beginIndex) else name) stripSuffix "$impl" stripSuffix "$backing"
      val initMethod = info.runtimeClass.getMethods.find(m => m.getName == pname + "$init")
      val pinfo = allInfos.collectFirst {
        case i
            if i.propertyMetadata.contains(pname) &&
              // we may have override @node val, should always pickup the last stored property definition
              // and check the initMethod to handle @transient val
              (i.propertyMetadata(pname).isStored || initMethod.isDefined) =>
          i.propertyMetadata(pname)
      }

      pinfo match {
        case None => propertyInfoNotFoundError(name, info.runtimeClass); None
        case Some(pinfo) =>
          val lookup = MethodHandles.lookup()
          p.setAccessible(true)
          val getField = resolveReader(lookup, pinfo, StorageKind.Val)
          val setField = resolveSetter(lookup, p, isVal = true)
          Some(UnsafeFieldInfo(pinfo, StorageKind.Val, initMethod, getField, setField))
      }
    }
  }
  private val validTimeType = methodType(classOf[Unit], classOf[BusinessEvent], classOf[java.time.Instant])
  def getValidTimeHandle(event: Class[_]): MethodHandle = {
    val field = event.getDeclaredField("validTime")
    field.setAccessible(true) // <- Handles allowing to write to final (non-static) field
    MethodHandles.lookup().unreflectSetter(field).asType(validTimeType)
  }
}
