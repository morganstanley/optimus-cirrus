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
package optimus.platform.storable

import optimus.platform._
import optimus.platform.pickling.CustomPicklingSpec
import optimus.platform.pickling.EmbeddablePicklers
import optimus.platform.pickling.PickledInputStream
import optimus.platform.pickling.PickledMapWrapper
import optimus.platform.pickling.PickledOutputStream
import optimus.platform.pickling.Pickler
import optimus.platform.pickling.PropertyMapOutputStream
import optimus.platform.pickling.Registry
import optimus.platform.pickling.Unpickler
import optimus.platform.util.ClassUtils

import scala.reflect.runtime.universe._

// pickled is `AnyRef` because a pickled embeddable can either be a `String` or a `Map[String, Any]`
@embeddable(customPickling = classOf[LazyEmbeddableWrapperCustomPicklingSpec.type])
final case class LazyEmbeddableWrapper[T <: Embeddable] private (pickled: AnyRef, tc: TemporalContext) {

  @volatile private[platform] var embeddable: T = _
  // does not currently support embeddables with generics - can be extended by using TypeTag or encoding type hierarchy
  // see: https://stackoverflow.com/questions/59473734/in-scala-2-12-why-none-of-the-typetag-created-in-runtime-is-serializable/
  @volatile private[platform] var knownPayloadType: Class[_ <: T] = _

  @node def payload: T = {
    // note that the @volatile read / write is racy, but not harmful - at worst we unpickle multiple times
    var e = embeddable
    if (e eq null) {
      // EmbeddablePicklers only relies on `temporalContext` from the pickled input stream, when deserializing nested
      // entities references
      val pickledInputStream = new PickledMapWrapper(Map.empty, temporalContext = tc)
      // guaranteed that `knownPayloadType` is not null by `LazyEmbeddableWrapperCustomPicklingSpec.unpickler`
      val tpe = ClassUtils.classToType(knownPayloadType)
      e = EmbeddablePicklers.unpicklerForType(tpe).unpickle(pickled, pickledInputStream).asInstanceOf[T]
      embeddable = e
    }
    e
  }
}

private object LazyEmbeddableWrapperCustomPicklingSpec extends CustomPicklingSpec[LazyEmbeddableWrapper[Embeddable]] {
  // pickles an LazyEmbeddableWrapper in exactly the same format as a direct embeddable, so you can switch a
  // field between E and LazyEmbeddableWrapper[E] with no DAL schema change
  override def pickler(tpe: Type): Pickler[LazyEmbeddableWrapper[Embeddable]] =
    (t: LazyEmbeddableWrapper[_], visitor: PickledOutputStream) => {
      // guaranteed to exist by `LazyEmbeddableWrapperCustomPicklingSpec.unpickler` OR `LazyEmbeddableWrapper.apply`
      visitor.writeRawObject(t.pickled)
    }
  // unpickles an LazyEmbeddableWrapper from exactly the same format as a direct embeddable, so you can switch a
  // field between E and LazyEmbeddableWrapper[E] with no DAL schema change
  override def unpickler(tpe: Type): Unpickler[LazyEmbeddableWrapper[Embeddable]] =
    (pickled: Any, ctxt: PickledInputStream) => {
      val holder = LazyEmbeddableWrapper[Embeddable](pickled.asInstanceOf[AnyRef], ctxt.temporalContext)
      val underlyingTpe :: Nil = tpe.typeArgs // the type `T` of `LazyEmbeddableWrapper[T]`, there should only be one
      holder.knownPayloadType = ClassUtils.typeToClass(underlyingTpe).asInstanceOf[Class[_ <: Embeddable]]
      holder
    }
}

object LazyEmbeddableWrapper {
  def apply[T <: Embeddable: TypeTag](e: T): LazyEmbeddableWrapper[T] = {
    val pickledValue = PropertyMapOutputStream.pickledValue(e, Registry.picklerOf[T])
    val holder = LazyEmbeddableWrapper[T](pickled = pickledValue.asInstanceOf[AnyRef], tc = null)
    holder.embeddable = e
    // no need to set `holder.knownPayloadType` because it is only accessed when `embeddable eq null`
    holder
  }
}
