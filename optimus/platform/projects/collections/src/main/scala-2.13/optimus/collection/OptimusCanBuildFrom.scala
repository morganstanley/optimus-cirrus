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
package optimus.collection

import java.util
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.compileTimeOnly
import scala.collection.BuildFrom
import scala.collection.IterableOnce
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.collection.mutable
import scala.reflect.ClassTag

private[collection] object OptimusCanBuildFrom {}
private[collection] abstract class OptimusCanBuildFrom[-From, -Elem, +To, ArrayType: ClassTag](
    override val toString: String,
    val empty: To,
    val settings: OSeqImpl
) extends BuildFrom[From, Elem, To] {
  type BuilderType <: OptimusBuilder[Elem, To]

  override def fromSpecific(from: From)(it: IterableOnce[Elem]): To = (makeBuilder() ++= it).result()
  override def newBuilder(from: From): BuilderType = makeBuilder()
  def newBuilder(): BuilderType = makeBuilder()

  /** its the same as apply, but the intent is to borrow/return, so its useful to separate the use case */
  private[collection] def borrowBuilder(): BuilderType = builderFromPooledOrMake()
  @deprecated("Not supported on scala 2.13.x")
  private[collection] def borrowIfPossible[From, Elem, To](
      bf: CanBuildFrom[From, Elem, To],
      coll: OSeq[_]): mutable.Builder[Elem, To] = ???

  protected def makeBuilder(): BuilderType

  private val builderPool = new AtomicReference[OptimusBuilder[_, _]]
  protected def builderFromPooledOrMake(): BuilderType = {
    val pooled = builderPool.getAndSet(null)
    if (pooled ne null) pooled.asInstanceOf[BuilderType] else makeBuilder()
  }
  protected val arrayPool = new AtomicReference[Array[ArrayType]]()

  protected trait OptimusBuilderImpl[-Elem, +To] extends OptimusBuilder[Elem, To] {

    protected var elems: Array[ArrayType]
    protected var arrays: Array[Array[ArrayType]] = null
    protected var knownHashes: Array[Int] = null
    protected var elemsIndex: Int = 0
    override private[collection] def resultSize: Int = {
      var result = 0
      if (arrays ne null) {
        var i = 0
        while (i < arrays.length) {
          if (arrays(i) ne null)
            result += arrays(i).length
          i += 1
        }
      }
      result += elemsIndex
      result
    }

    // its virtual here, as the bimorphic dispatch is cheaper then the scala array length on a generic array
    // = if (elems eq null) 0 else elems.length
    protected def capacity: Int
    // effectively new Array[ArrayType](settings.maxArraySize)
    protected def newArray(size: Int): Array[ArrayType]
    // copyOf, but we cant do that on scala generic arrays
    protected def copyToArray(): Array[ArrayType]

    protected def mkEmptyArray(): Array[ArrayType] = {
      var newelems: Array[ArrayType] = arrayPool.getAndSet(null)
      if (newelems eq null) newelems = new Array[ArrayType](settings.maxArraySize)
      newelems
    }

    override final def sizeHint(size: Int) = {
      val additional = resultSize - size
      if (additional > 0) prepareForAdditional(size)
    }

    protected def prepareForAdditional(newElementsCount: Int) = {
      val cap = capacity
      if (cap - elemsIndex < newElementsCount && elemsIndex > settings.minArraySize) {
        flushToArrays()
      }
      if (newElementsCount > 0 && (elems eq null)) {
        elems = mkEmptyArray()
      }
      capacity - elemsIndex
    }
    protected def flushToArrays(): Unit = {
      if (elemsIndex > 0) {
        val nextIndex = ensureArrays(1)
        if (elemsIndex == capacity) {
          arrays(nextIndex) = elems
          elems = mkEmptyArray()
        } else {
          arrays(nextIndex) = copyToArray()
          clearRefsInArray(elems, elemsIndex)
        }
        elemsIndex = 0
      }
    }

    protected def trimArrays(): Unit = {
      assert(arrays ne null)
      var usedSize = arrays.length
      while (usedSize > 0 && ((arrays(usedSize - 1)) eq null)) usedSize -= 1
      if (usedSize < arrays.length)
        arrays = util.Arrays.copyOf(arrays, usedSize)
    }
    protected def ensureArrays(additionalSize: Int): Int = {
      if (arrays eq null) {
        arrays = new Array[Array[ArrayType]](Math.max(4, additionalSize))
        0
      } else {
        var nextFree = arrays.length
        while (nextFree > 0 && ((arrays(nextFree - 1)) eq null)) nextFree -= 1
        if (nextFree + additionalSize > arrays.length)
          arrays = util.Arrays.copyOf(arrays, nextFree + Math.max(4, additionalSize))
        nextFree
      }
    }
    override final private[collection] def addFromArray(data: Array[_], start: Int, end: Int): Unit = {
      if (implicitly[ClassTag[ArrayType]].runtimeClass.isAssignableFrom(data.getClass.getComponentType))
        copyFromArray(data.asInstanceOf[Array[_ <: ArrayType]], start, end)
      else {
        val slowData = new Array[ArrayType](end)
        mutable.WrappedArray.make(data).copyToArray(slowData.asInstanceOf[Array[Any]])
        copyFromArray(slowData.asInstanceOf[Array[_ <: ArrayType]], start, end)
      }
    }
    final private[collection] def copyFromArray(data: Array[_ <: ArrayType], start: Int, end: Int): Unit = {
      prepareForAdditional(end - start)
      var toCopy = end - start
      var sourceStart = start
      while (toCopy > 0) {
        val thisBlockLength = Math.min(toCopy, elems.length - elemsIndex)
        System.arraycopy(data, sourceStart, elems, elemsIndex, thisBlockLength)
        elemsIndex += thisBlockLength
        toCopy -= thisBlockLength
        sourceStart += thisBlockLength
        if (toCopy > 0) flushToArrays()
      }
    }

    override def addFrom(data: collection.Seq[Elem], start: Int, end: Int): Unit = {
      prepareForAdditional(end - start)
      var toCopy = end - start
      var sourceStart = start
      while (toCopy > 0) {
        val thisBlockLength = Math.min(toCopy, elems.length - elemsIndex)
        data.copyToArray(elems.asInstanceOf[Array[Elem]], elemsIndex, thisBlockLength)
        elemsIndex += thisBlockLength
        toCopy -= thisBlockLength
        sourceStart += thisBlockLength
        if (toCopy > 0) flushToArrays()
      }
    }

    override private[collection] def addSharedArray(data: Array[_]): Unit = {
      if (implicitly[ClassTag[ArrayType]].runtimeClass.isAssignableFrom(data.getClass.getComponentType)) {
        flushToArrays()
        val nextIndex = ensureArrays(1)
        arrays(nextIndex) = data.asInstanceOf[Array[ArrayType]]
      } else {
        // should not happen
        addFromArray(data, 0, data.length)
      }
    }
    def addArray(anArray: Array[ArrayType]): Unit = {
      assert(elemsIndex == 0)
      val index = ensureArrays(1)
      arrays(index) = anArray
    }
    def clear(): Unit = {
      clearRefsInArray(elems, elemsIndex)
      elemsIndex = 0
      if (arrays ne null)
        util.Arrays.fill(arrays.asInstanceOf[Array[AnyRef]], null)
    }

    override private[collection] final def returnBorrowed(): Unit = {
      if (builderPool.get() eq null) {
        clear()
        builderPool.set(this.asInstanceOf[BuilderType])
      }
    }

    def clearRefsInArray(array: Array[ArrayType], maxIndex: Int) = ()

    protected def returnElemsArrayIfAppropriate(): Unit = {
      if ((elems ne null) && (arrayPool.get eq null)) {
        clearRefsInArray(elems, elemsIndex)
        arrayPool.set(elems)
      }
      elems = null
      elemsIndex = 0
    }

  }
}
object OptimusBuilder {
  def returnIfBorrowed[To](builder: mutable.Builder[_, To]): Unit = builder match {
    case internal: OptimusBuilder[_, To] =>
      internal.returnBorrowed()
    case _ =>
  }
}
abstract class OptimusBuilder[-Elem, +To] extends mutable.Builder[Elem, To] {

  def addFrom(data: collection.Seq[Elem], start: Int, end: Int): Unit
  def addOne(elem1: Elem, elem2: Elem, elems: Elem*): this.type = addOne(elem1).addOne(elem2).addAll(elems)
  private[collection] def resultSize: Int
  private[collection] def addFromArray(data: Array[_], start: Int, end: Int): Unit
  // allows structural sharing of arrays. Only used by higher order functions in ArraysSeq
  private[collection] def addSharedArray(data: Array[_]): Unit

  private[collection] def returnBorrowed(): Unit
  override def result(): To
}
abstract class OptimusDoubleBuilder[+To] extends OptimusBuilder[Double, To] {
  // overridden to stop boxing
  override def addOne(elem: Double): this.type
  override def addOne(elem1: Double, elem2: Double, elems: Double*): this.type =
    addOne(elem1).addOne(elem2).addAll(elems)
}
