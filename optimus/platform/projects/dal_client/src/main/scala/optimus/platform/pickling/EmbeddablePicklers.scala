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

import optimus.core.NodeAPI
import optimus.core.utils.RuntimeMirror
import optimus.exceptions.RTExceptionTrait
import optimus.graph.AlreadyCompletedNode
import optimus.graph.CompletableNodeM
import optimus.graph.Node
import optimus.graph.NodeFuture
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.platform.EvaluationContext
import optimus.platform.EvaluationQueue
import optimus.platform.annotations.nodeSync
import optimus.platform.dal.StorableSerializer
import optimus.platform.pickling.PicklingReflectionUtils._
import optimus.platform.storable.Embeddable
import optimus.platform.storable.EmbeddableCompanionBase
import optimus.platform.storable.EmbeddableTraitCompanionBase
import optimus.platform.storable.HasDefaultUnpickleableValue

import scala.collection.compat._
import scala.collection.immutable.SortedSet
import scala.util.Try
import scala.reflect.runtime.universe.{Try => _, _}
import optimus.scalacompat.collection._

object EmbeddablePicklers extends StorableSerializer {

  /** _tag field contains the simple classname for pickled @embedable case classes */
  val Tag = "_tag"

  private val EmbeddableType = typeOf[Embeddable]
  def picklerForType(tpe: Type): Pickler[_] = {
    def module = {
      val companionName = tpe.typeSymbol.companion match {
        case NoSymbol =>
          // sometimes the companion isn't found by scala reflection, but forcing reflection to resolve all of the
          // siblings first makes it work
          // TODO (OPTIMUS-63850): Investigate the bug in scala reflection library and remove this workaround
          tpe.typeSymbol.owner.typeSignature.decls
          tpe.typeSymbol.companion
        case t => t
      }
      RuntimeMirror.moduleByName(companionName.fullName, getClass)
    }
    if (tpe =:= EmbeddableType) {
      throw new IllegalArgumentException(
        s"$EmbeddableType is not directly pickleable - you must use a specific @embeddable trait/class")
    } else if (tpe.typeSymbol.isAbstract) {
      picklerForTrait(module.asInstanceOf[EmbeddableTraitCompanionBase])
    } else if (tpe.typeSymbol.isModuleClass) {
      new EmbeddableObjectPickler(tpe.typeSymbol.name.encodedName.toString)
    } else {
      new EmbeddablePickler(module.asInstanceOf[EmbeddableCompanionBase], tpe)
    }
  }

  def picklerForTrait(etcb: EmbeddableTraitCompanionBase): Pickler[_] = {
    class TraitPickler extends Pickler[Embeddable] {
      lazy val picklers: Map[String, Try[Pickler[Embeddable]]] =
        etcb.subtypeSimpleNameToClass.mapValuesNow(cls =>
          Try(Registry.picklerOfType(PicklingReflectionUtils.classToType(cls)).asInstanceOf[Pickler[Embeddable]]))

      def pickle(t: Embeddable, visitor: PickledOutputStream): Unit = {
        val clsName = t.getClass.getSimpleName.stripSuffix("$")
        picklers.get(clsName) match {
          case Some(p) => p.get.pickle(t, visitor)
          case None =>
            val tpeParamNames = t.getClass.getTypeParameters.map(_.toString)
            val expectationMsg = if (etcb.subtypeSimpleNameToClass.contains(clsName) && tpeParamNames.nonEmpty) {
              s"cannot find pickler for unbounded type(s) ${tpeParamNames.mkString(", ")} in $clsName"
            } else expectedSubtypesMessage(etcb)
            throw new IllegalArgumentException(s"Found instance of ${t.getClass}, but $expectationMsg")
        }
      }
    }

    new TraitPickler
  }

  def unpicklerForType(tpe: Type): Unpickler[_] = {
    val module = PicklingReflectionUtils.companionObjectInstance(tpe)
    if (tpe.typeSymbol.isAbstract) {
      unpicklerForTrait(module.asInstanceOf[EmbeddableTraitCompanionBase])
    } else if (tpe.typeSymbol.isModuleClass) {
      new EmbeddableObjectUnpickler(module.asInstanceOf[Embeddable])
    } else {
      new EmbeddableUnpickler(module.asInstanceOf[EmbeddableCompanionBase], tpe)
    }
  }

  def unpicklerForTrait(etcb: EmbeddableTraitCompanionBase): Unpickler[_] = {
    new TraitUnpickler(etcb)
  }

  class TraitUnpickler(val etcb: EmbeddableTraitCompanionBase) extends Unpickler[Embeddable] {
    private[optimus] lazy val unpicklers: Map[String, Try[Unpickler[Embeddable]]] =
      etcb.subtypeSimpleNameToClass.mapValuesNow(cls =>
        Try(Registry.unpicklerOfType(PicklingReflectionUtils.classToType(cls)).asInstanceOf[Unpickler[Embeddable]]))

    def unpicklerForSubtype(simpleName: String): Unpickler[Embeddable] = unpicklers(simpleName).get

    private def unpickleTrait(clsName: String, m: Any, is: PickledInputStream): NodeFuture[Embeddable] = {
      unpicklers.get(clsName) match {
        case Some(u) => u.get.unpickle$queued(m, is)
        case None =>
          etcb match {
            case o: HasDefaultUnpickleableValue[Embeddable] @unchecked =>
              new AlreadyCompletedNode(o.defaultUnpickleableValue)
            case _ =>
              throw new UnexpectedUnpickleObjectException(clsName, s"; ${expectedSubtypesMessage(etcb)}")
          }
      }
    }

    @nodeSync def unpickle(m: Any, is: PickledInputStream): Embeddable = unpickle$queued(m, is).get$
    override def unpickle$queued(m: Any, is: PickledInputStream): NodeFuture[Embeddable] = {
      m match {
        case e: Embeddable => new AlreadyCompletedNode(e)
        case _ =>
          val clsName = extractSimpleClassname(m)
          unpickleTrait(clsName, m, is)
      }
    }
  }

  def extractSimpleClassname(pickled: Any): String = {
    val clsName = pickled match {
      // @embeddable case classes get pickled as a map with _tag containing the simple classname
      case m: Map[String, String] @unchecked => m(Tag)
      // @embeddable case object is pickled as a bare String of the simple classname (see EmbeddableObjectPickler)
      case objName: String => objName
      // This to support OPTIMUS-9680
      case _: Embeddable => pickled.getClass.getSimpleName
    }
    clsName
  }

  class EmbeddablePickler(val ecb: EmbeddableCompanionBase, tpe: Type) extends Pickler[Embeddable] {
    val name = getEncodedName(tpe)

    private lazy val picklers: Array[(String, Pickler[_])] = {
      val ctorArgs = extractCtorArgs(tpe)
      ctorArgs.map { case (ctorName, ctorType) =>
        try ctorName -> Registry.picklerOfType(ctorType)
        catch {
          case e: Exception =>
            throw new IllegalArgumentException(
              s"Unable to find pickler for field $ctorName (of type $ctorType) of @embeddable $name",
              e)
        }
      }.toArray
    }

    def pickle(t: Embeddable, os: PickledOutputStream): Unit = {
      val values = ecb.toArray(t)
      os.writeStartObject()
      os.writeFieldName(Tag)
      os.write(name, DefaultPicklers.stringPickler)

      var i = 0
      while (i < picklers.length) {
        val (fname, pickler: Pickler[AnyRef] @unchecked) = picklers(i)
        os.writeFieldName(fname)
        os.write(values(i), pickler)
        i += 1
      }
      os.writeEndObject()
    }
  }

  class EmbeddableUnpickler(val ecb: EmbeddableCompanionBase, tpe: Type) extends Unpickler[Embeddable] {
    val name = extractName(tpe)
    lazy val unpicklers: Array[(String, Unpickler[_])] = {
      val ctorArgs = extractCtorArgs(tpe)
      ctorArgs.map { case (ctorName, ctorType) =>
        try ctorName -> Registry.unpicklerOfType(ctorType)
        catch {
          case e: Exception =>
            throw new IllegalArgumentException(
              s"Unable to find unpickler for field $ctorName (of type $ctorType) of @embeddable $name",
              e)
        }
      }.toArray
    }

    @nodeSync def unpickle(pickled: Any, is: PickledInputStream): Embeddable = unpickle$queued(pickled, is).get$

    override def unpickle$queued(pickled: Any, is: PickledInputStream): NodeFuture[Embeddable] = pickled match {
      case e: Embeddable => new AlreadyCompletedNode(e)
      case _ =>
        new CompletableNodeM[Embeddable] {
          private var waitingOn = 0 // Current/last sub unpickler we are waiting on....
          private var arr: Array[AnyRef] = _ // Waiting on list to resolve values
          private var versionResolver: Node[Map[String, Any]] = _
          private var propMap: Map[String, Any] = _

          enqueueChildrenOrComplete()

          def enqueueChildrenOrComplete(): Unit = {
            pickled match {
              case m: Map[String, Any] @unchecked =>
                initAsRunning(EvaluationContext.scenarioStack)
                arr = new Array[AnyRef](unpicklers.length)
                propMap = m - Tag
                versionResolver = NodeAPI.queuedNodeOf(
                  version(ecb.shapeName, propMap, is.temporalContext, applyForceTransformation = false))
                versionResolver.continueWith(this, OGSchedulerContext.current())
              case o: Embeddable => initAsCompleted(o) // Assume it's already the right type
            }
          }

          override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = ??? /* {
            try {
              if (child eq versionResolver) {
                val versionedPropMap = versionResolver.get
                var i = 0
                while (i < arr.length) {
                  val (fname, unpickler: Unpickler[Any]) = unpicklers(i)
                  // noinspection GetGetOrElse don't want to create lambda for each invocation when using Map#getOrElse
                  val fieldValue = versionedPropMap.get(fname) getOrElse {
                    throw new IncompatibleEmbeddableVersionException(
                      embeddableClassName = ecb.shapeName,
                      firstFailureField = fname,
                      foundFields = propMap.keys.to(SortedSet),
                      requiredFields = unpicklers.map { case (fieldName, _) => fieldName }.to(SortedSet)
                    )
                  }
                  arr(i) = EvaluationContext.asIfCalledFrom(this, eq)(unpickler.unpickle$queued(fieldValue, is))
                  i += 1
                }
              }

              // Consume all completed nodes
              var i = waitingOn // When found first incomplete node
              while (i < unpicklers.length) {
                arr(i) match {
                  case n: Node[_] =>
                    if (n.isDone) { arr(i) = n.resultObject; i += 1 }
                    else {
                      waitingOn = i; i = Int.MaxValue; n.continueWith(this, eq)
                    }
                  case _ => i += 1
                }
              }
              if (i == unpicklers.length)
                completeWithResult(ecb.fromArray(arr), eq)
            } catch { case e: Throwable => completeWithException(e, eq) }
          } */
        }
    }
  }

  // @embeddable objects are written simply as the classname (unwrapped, NOT inside a "_tag" field)
  class EmbeddableObjectPickler(name: String) extends Pickler[Embeddable] {
    override def pickle(t: Embeddable, visitor: PickledOutputStream): Unit =
      visitor.writeRawObject(name)
  }

  class EmbeddableObjectUnpickler(val obj: Embeddable) extends Unpickler[Embeddable] {
    override def unpickle(pickled: Any, ctxt: PickledInputStream): Embeddable = obj
    override def unpickle$queued(pickled: Any, ctxt: PickledInputStream): NodeFuture[Embeddable] =
      new AlreadyCompletedNode[Embeddable](obj)
  }

  private def expectedSubtypesMessage(etcb: EmbeddableTraitCompanionBase) = {
    val subtypes = etcb.subtypeSimpleNameToClass.keys.toSeq.sorted.mkString(" or ")
    s"expected subtype of ${etcb.getClass.getName.stripSuffix("$")} (i.e. $subtypes)"
  }
}

final class IncompatibleEmbeddableVersionException private (
    details: String,
    val embeddableClassName: String,
    val firstFailureField: String,
    // sort fields to ensure RT: their contents becomes part of exception messages and those can end up returned in
    // a Result from @nodes
    val foundFields: SortedSet[String],
    val requiredFields: SortedSet[String])
// extends NoSuchElementException to preserve client incompatible versions handling code
    extends NoSuchElementException(details)
    with RTExceptionTrait {

  // using chaining ctor rather than companion apply as exceptions are very commonly instantiated in throw new E pattern
  // so it's likely the apply method will go unnoticed and clients will be constructing strings manually
  def this(
      embeddableClassName: String,
      firstFailureField: String,
      foundFields: SortedSet[String],
      requiredFields: SortedSet[String],
      customDetails: Option[String] = None) =
    this(
      details = customDetails.getOrElse(
        s"key not found for $firstFailureField while unpickling $embeddableClassName\nmissing fields=[${(requiredFields -- foundFields)
            .mkString(",")}]\nextra fields=[${(foundFields -- requiredFields).mkString(",")}] (extra generally not relevant)\nfields found in incoming properties map=[${foundFields
            .mkString(",")}]"),
      embeddableClassName = embeddableClassName,
      firstFailureField = firstFailureField,
      foundFields = foundFields,
      requiredFields = requiredFields
    )

  def missingFields: SortedSet[String] = requiredFields -- foundFields
  def extraFields: SortedSet[String] = foundFields -- requiredFields
}
