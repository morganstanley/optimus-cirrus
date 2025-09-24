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

import optimus.graph.Settings
import org.objectweb.asm.Type

import java.io.ObjectInput
import java.lang.invoke.MethodHandle
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/*
 * Shape represents the structure of a SlottedBuffer. It contains the names and the types of each field. A SlottedBuffer
 * corresponding to a given Shape is created using Shape.createInstance. The SlottedBuffer maintains a reference to
 * the Shape it was created from which is used to resolve the field names to index of each of the fields.
 */
abstract private[platform] class ShapeBase {
  def classes: Array[Class[_]]
  def names: Array[String]
  def isSeq: Boolean = names eq null
  def signature: String = Shape.signatureFromClasses(classes, isSeq)
}

@SerialVersionUID(1L)
private[platform] final class Shape private (
    override val names: Array[String],
    override val classes: Array[Class[_]],
    val tag: String // This is the Embeddable tag if this Shape is for an embeddable
) extends ShapeBase
    with Serializable
    with Cloneable {

  private lazy val maybeInterner = StatsBasedInterner(classes.length, Settings.InterningScope.PICKLED)

  private val sbCtors = SlottedBufferClassGenerator.getOrResolveSBCtors(this)

  private val name2index: Map[String, Int] =
    if (isSeq) Map.empty else PicklingMapEntryOrderWorkaround.newMapForNames(names, hasTag)

  val fieldOrder: Array[(String, Int)] = name2index.toArray
  def hasTag: Boolean = tag ne Shape.NoTag

  // This is used to support removal of the tag from SlottedBufferAsMap methods.
  // noinspection ScalaUnusedSymbol - called by generated code
  def maybeIntern(o: AnyRef, index: Int): AnyRef = {
    if (!safeToIntern || isSeq) o
    else maybeInterner.maybeIntern(o, index)
  }

  def getFieldIndex(key: String): Int = name2index.getOrElse(key, Shape.invalidField)

  def createInstanceAsMap(args: Array[Any]): SlottedBufferAsMap = {
    sbCtors.mainCtor.invokeExact(args, this): SlottedBufferAsMap
    // Type ascription required so scalac generates the correct invocation
  }

  def createInstanceAsSeq(args: Array[Any]): SlottedBufferAsSeq = {
    sbCtors.mainCtor.invokeExact(args): SlottedBufferAsSeq
    // Type ascription required so scalac generates the correct invocation
  }

  def readInstanceAsMap(in: ObjectInput): SlottedBufferAsMap = {
    sbCtors.streamCtor.invokeExact(in, this): SlottedBufferAsMap
    // Type ascription required so scalac generates the correct invocation
  }

  def readInstanceAsSeq(in: ObjectInput): SlottedBufferAsSeq = {
    sbCtors.streamCtor.invokeExact(in): SlottedBufferAsSeq
    // Type ascription required so scalac generates the correct invocation
  }

  // Serialization Support
  def writeReplace(): AnyRef = {
    new ShapeMoniker(this)
  }

  // The rationale for using a var and cloning is because we want the non-interning
  // instance of the shape to share all its state except for the safeToInternSbs flag.
  // Sharing the state is important because we want statistics on queries and hits
  // for each of the fields to drive the interning decisions of each of the fields
  // of the shape.
  private var allowInterning: Boolean = true
  // This is set to true if all the fields of the shape are internable
  def safeToIntern: Boolean = allowInterning
  lazy val nonInterning: Shape = {
    val other = this.clone().asInstanceOf[Shape]
    other.allowInterning = false
    other
  }
}

// We cache Shape instances as they are highly-reused. To lookup a shape, we could have used a Shape instance
// but they have the overhead of allocating the class members associated with stats collection so we define
// a stripped-down ShapeKey class for the purposes of lookup in the cache
private class ShapeKey(override val names: Array[String], override val classes: Array[Class[_]], val tag: String)
    extends ShapeBase {

  override def hashCode(): Int = {
    var h = 0
    var i = 0
    while (i < classes.length) {
      h = 31 * h + classes(i).hashCode
      if (names != null)
        h = 27 * h + names(i).hashCode
      i += 1
    }
    h
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ShapeKey =>
        if (this eq that) true
        else if (this.tag != that.tag) false
        else if (this.classes.length != that.classes.length) false
        else {
          var i = 0
          if (this.names eq null) that.names eq null
          else if (that.names eq null) this.names eq null
          else {
            var diff = false
            while (i < this.names.length && !diff) {
              if (this.names(i) != that.names(i)) diff = true
              if (this.classes(i) != that.classes(i)) diff = true
              i += 1
            }
            !diff
          }
        }
      case _ => false
    }
  }
}

private[platform] object Shape {

  // important that this is new String rather than a string literal.
  // It's used a marker for an 'eq' check.
  val NoTag = new String("__NoTag__")
  val invalidField: Int = Int.MinValue
  private val shapeCache = new ConcurrentHashMap[ShapeKey, Shape]()

  // This creates a Shape to be used for SlottedBufferAsMap
  def apply(names: Array[String], classes: Array[Class[_]], tag: String, safeToIntern: Boolean): Shape = {
    val lookupShape = new ShapeKey(names, classes, tag)
    val shape = shapeCache.computeIfAbsent(lookupShape, k => new Shape(k.names, k.classes, k.tag))
    if (safeToIntern) shape else shape.nonInterning
  }

  // This creates a Shape to be used for SlottedBufferAsSeq
  def apply(classes: Array[Class[_]]): Shape = apply(null, classes, Shape.NoTag, safeToIntern = true)

  // Intended to be used from the debugger to dump out stats.
  def dumpStats(): String = {
    val sb = new StringBuilder()
    sb.append("field,queries,hits,sameDecisionCnt,flips,lastDecision\n")
    shapeCache.values().asScala.foreach { shape =>
      if (!shape.isSeq) {
        var i = 0
        while (i < shape.classes.length) {
          if (!shape.classes(i).isPrimitive) {
            sb.append(shape.signature)
            sb.append("-")
            sb.append(if (shape.isSeq) i else shape.names(i))
            shape.maybeInterner.dumpStats(i, sb)
            sb.append("\n")
          }
          i += 1
        }
      }
    }
    sb.toString
  }

  def reachedUniqueShapeThreshold: Boolean = {
    shapeCache.values.size >= Settings.uniqueShapeThreshold
  }

  def classesFromSignature(signature: String): Array[Class[_]] = {
    // Signature is in the form of "M2IJDL" which means 2 integers, 1 long, 1 double and 1 object
    var repetition: StringBuilder = null
    val classes = new ArrayBuffer[Class[_]](signature.length - 1)

    def addN(cls: Class[_]): Unit = {
      val reps = if (repetition != null) repetition.toString.toInt else 1
      var j = 0
      while (j < reps) {
        classes.addOne(cls)
        j += 1
      }
      repetition = null
    }

    // Skip first character ('M' or 'S')
    var i = 0
    while (i < signature.length - 1) {
      signature(i + 1) match {
        case 'I' => addN(classOf[Int])
        case 'D' => addN(classOf[Double])
        case 'Z' => addN(classOf[Boolean])
        case 'C' => addN(classOf[Char])
        case 'J' => addN(classOf[Long])
        case 'S' => addN(classOf[Short])
        case 'B' => addN(classOf[Byte])
        case 'L' => addN(classOf[AnyRef])
        case digit =>
          if (repetition eq null)
            repetition = new StringBuilder()
          repetition.append(digit)
      }
      i += 1
    }
    classes.toArray
  }

  def signatureFromClasses(classes: Array[Class[_]], isSeq: Boolean): String = {
    val sb = new StringBuilder(if (isSeq) "S" else "M")
    var counter = 0
    def append(str: String): Unit = {
      if (counter > 1) sb.append(counter)
      sb.append(str)
      counter = 1
    }
    var last = ""
    var i = 0
    while (i < classes.length) {
      val cls = classes(i)
      val sym = if (cls.isPrimitive) Type.getDescriptor(cls) else "L"
      if (sym == last || last.isBlank) {
        counter += 1
      } else append(last)
      last = sym
      i += 1
      if (i == classes.length) append(sym)
    }
    sb.toString()

  }

}

final case class SlottedBufferCtorHolder(mainCtor: MethodHandle, streamCtor: MethodHandle)
