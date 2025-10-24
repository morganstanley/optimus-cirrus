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

import optimus.core.InternerHashEquals

import java.io.ObjectOutput
import scala.collection.AbstractIterator

/*
 * SlottedBuffer is the base class for runtime-generated shape-specific classes that are intended to hold the
 * pickled form of entities and embeddables in a more memory-efficient manner than a Map. It extends Map so that
 * consuming code can stay as-is for the most part. But the longer term intent is for those layers to be changed
 * to use SlottedBuffers more directly, avoiding the get(key) calls as these incur the cost of a hash lookup
 */
@SerialVersionUID(1L)
abstract class SlottedBufferAsMap(private[pickling] val shape: Shape)
    extends PickledProperties
    with ContainsUnresolvedReference
    with InternerHashEquals
    with Serializable {

  override def apply(key: String): Any = get(key).getOrElse { throw new NoSuchElementException }

  protected[pickling] def get(index: Int): Any

  override def hasTag: Boolean = shape.hasTag
  override def tag: String = if (hasTag) shape.tag else throw new NoSuchElementException("No tag present")
  override def untagged: TaglessSlottedBufferAsMap = new TaglessSlottedBufferAsMap(this)

  override def get(key: String): Option[Any] = {
    if (shape.hasTag && key == PicklingConstants.embeddableTag)
      Some(shape.tag)
    else {
      val idx = shape.getFieldIndex(key)
      if (idx == Shape.invalidField) None else Some(get(idx))
    }
  }

  override def iterator: Iterator[(String, Any)] = new AbstractIterator[(String, Any)] {
    private var index = 0
    override def hasNext: Boolean = shape.fieldOrder.length > index
    override def next(): (String, Any) = {
      val (fieldName, fieldIndex) = shape.fieldOrder(index)
      val v =
        if (fieldIndex == -1)
          (PicklingConstants.embeddableTag, shape.tag)
        else
          (fieldName, get(fieldIndex))
      index += 1
      v
    }
  }

  override def knownSize: Int = {
    if (shape.hasTag) shape.names.length + 1
    else shape.names.length
  }

  // We don't want to override hashCode as it won't match hashCode of scala.collection.Seq
  // so we provide our own to specifically be used for interning
  override def internerHashCode(): Int = {
    shape.hashCode() * 31 + argsHash()
  }

  override def internerEquals(obj: Any): Boolean = {
    obj match {
      case that: SlottedBufferAsMap =>
        if (this eq that) return true
        // note that equal shapes implies equal generated SBM subclasses, so the argsEqual call is safe
        if (shape != that.shape) return false
        argsEqual(that)
      case _ => false
    }
  }

  // Implementations are generated in derived classes
  protected def argsHash(): Int
  protected def argsEqual(other: Any): Boolean

  //
  // Serialization Support
  //
  // noinspection ScalaUnusedSymbol
  def writeReplace(): AnyRef = {
    new SlottedBufferAsMapMoniker(this)
  }
  // Generated classes will implement this. It'll be called from the above moniker
  def write(out: ObjectOutput): Unit
  def signature: String = shape.signature
}

// During actual embeddable unpickling, the tag in the pickled form is removed before it
// gets passed into versioning and the unpicklers. This class is used to wrap the underlying
// SlottedBufferAsMap in a way that filters out the tag from the interface while still
// extending Map[String, Any]
class TaglessSlottedBufferAsMap(private val underlying: SlottedBufferAsMap)
    extends PickledProperties
    with ContainsUnresolvedReference
    with InternerHashEquals {

  override def get(key: String): Option[Any] = {
    if (key == PicklingConstants.embeddableTag)
      None
    else
      underlying.get(key)
  }
  override def iterator: AbstractIterator[(String, Any)] =
    new AbstractIterator[(String, Any)] {
      private val underlyingIt = underlying.iterator
      private var nextElement: (String, Any) = fetchNext()

      private def fetchNext(): (String, Any) = {
        while (underlyingIt.hasNext) {
          val elem = underlyingIt.next()
          if (elem._1 != PicklingConstants.embeddableTag) {
            return elem
          }
        }
        TaglessSlottedBufferAsMap.NoMoreElements
      }

      override def hasNext: Boolean = nextElement ne TaglessSlottedBufferAsMap.NoMoreElements

      override def next(): (String, Any) = {
        if (!hasNext) throw new NoSuchElementException("No more elements")
        val result = nextElement
        nextElement = fetchNext()
        result
      }
    }

  override def internerHashCode(): Int = {
    underlying.internerHashCode()
  }

  override def internerEquals(obj: Any): Boolean = {
    obj match {
      case that: TaglessSlottedBufferAsMap =>
        if (this eq that) return true
        underlying.internerEquals(that.underlying)
      case _ => false
    }
  }
}

private object TaglessSlottedBufferAsMap {
  private val NoMoreElements: (String, Any) = null
}
