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
import optimus.platform.pickling.PropertyMapOutputStream.PickleSeq

import java.io.ObjectOutput
import scala.collection.AbstractIterator

@SerialVersionUID(1L)
abstract class SlottedBufferAsSeq
    extends PickleSeq[Any]
    with ContainsUnresolvedReference
    with InternerHashEquals
    with Serializable {

  protected[pickling] def get(index: Int): Any

  override def iterator: Iterator[Any] = new AbstractIterator[Any] {
    private var index = 0
    override def hasNext: Boolean = SlottedBufferAsSeq.this.length > index
    override def next(): Any = {
      val i = index
      index += 1
      get(i)
    }
  }

  override def apply(i: Int): Any = get(i)

  // We don't want to override hashCode as it won't match hashCode of scala.collection.Seq
  // so we provide our own to specifically be used for interning
  override def internerHashCode(): Int = getClass.getName.hashCode + 31 * argsHash()

  override def internerEquals(obj: Any): Boolean = {
    if (obj == null || (obj.getClass ne getClass)) false
    else if (this eq obj.asInstanceOf[AnyRef]) true
    else argsEqual(obj)
  }

  // Implementations are generated in derived classes
  protected def argsHash(): Int
  protected def argsEqual(other: Any): Boolean

  //
  // Serialization Support
  //
  // noinspection ScalaUnusedSymbol
  def writeReplace(): AnyRef = {
    new SlottedBufferAsSeqMoniker(this)
  }
  // Generated classes will implement these. It'll be called from the above moniker
  def write(out: ObjectOutput): Unit
  def signature: String
}

@SerialVersionUID(1L)
object SlottedBufferAsEmptySeq extends PickleSeq[Any] with ContainsUnresolvedReference with Serializable {
  override def apply(i: Int): Any = throw new UnsupportedOperationException()
  override def length: Int = 0
  override def iterator: Iterator[Any] = new AbstractIterator[Any] {
    override def hasNext: Boolean = false
    override def next(): Any = throw new NoSuchElementException("No more elements")
  }
}
