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
package optimus.platform.relational.tree

import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import java.nio.ByteBuffer

trait Packer[-T] { def serialize(t: T, state: Packer.WriteState): Unit }
trait Unpacker[+T] { def deserialize(state: Unpacker.ReadState): T }

/**
 * Packing, designed primarily for efficiently serializing TypeInfos to bytes, though adaptable for other purposes by
 * providing a Packing type class for that type.
 *
 * The type class for TypeInfos is typical of serialization for a case class, so can be taken as a template for other
 * implementations.
 */
object Packer {

  def apply[T: Packer](t: T): Array[Byte] = {
    val state = new WriteState
    implicitly[Packer[T]].serialize(t, state)
    state.buf.toArray
  }

  class WriteState {
    protected[Packer] val cache = new ListBuffer[String]()
    val buf = new ArrayBuffer[Byte]
    def put(b: Byte*): Unit = b foreach (buf += _)
  }

  /**
   * Packs sequences, much like the way strings are packed.
   */
  implicit def seqPacking[T: Packer]: Packer[Seq[T]] = new Packer[Seq[T]] {
    def serialize(t: Seq[T], state: WriteState): Unit = {
      if (t.length > 127) state.put(-128.toByte, ((t.length >> 8) & 0xff).toByte)
      state.put((t.length & 0xff).toByte)
      t foreach { v =>
        implicitly[Packer[T]].serialize(v, state)
      }
    }
  }

  implicit val intPacker: Packer[Int] = new Packer[Int] {
    def serialize(i: Int, state: WriteState): Unit = {
      if (i >= 0 && i < 254) state.put(i.toByte)
      else
        state.put(
          0xff.toByte,
          ((i >> 24) & 0xff).toByte,
          ((i >> 16) & 0xff).toByte,
          ((i >> 8) & 0xff).toByte,
          (i & 0xff).toByte)
    }
  }

  /**
   * Packs strings. Strings are stored as an initial byte, normally representing the length of the string, followed by
   * the bytes of the string. If the length byte is 0xff, this indicates that the string is longer than 127 characters,
   * and the subsequent two bytes will represent the length, as a 16-bit integer. If the length byte is any other
   * negative number, it is considered a back-reference to a string which has been previously packed/unpacked.
   */
  implicit val stringPacker: Packer[String] = new Packer[String] {
    def serialize(t: String, state: WriteState): Unit = {
      if (state.cache contains t) state.put((-state.cache.indexOf(t) - 1).toByte)
      else {
        if (t.length > 127) state.put(-128.toByte, ((t.length >> 8) & 0xff).toByte)
        state.cache += t
        state.put((t.length & 0xff).toByte)
        state.put(t.getBytes("ASCII").toVarArgsSeq: _*)
      }
    }
  }

  implicit def tuplePacker[T: Packer, U: Packer]: Packer[(T, U)] = new Packer[(T, U)] {
    def serialize(t: (T, U), state: WriteState): Unit = {
      implicitly[Packer[T]].serialize(t._1, state)
      implicitly[Packer[U]].serialize(t._2, state)
    }
  }

  implicit def tuple3Packer[T: Packer, U: Packer, V: Packer]: Packer[(T, U, V)] = new Packer[(T, U, V)] {
    def serialize(t: (T, U, V), state: WriteState): Unit = {
      implicitly[Packer[T]].serialize(t._1, state)
      implicitly[Packer[U]].serialize(t._2, state)
      implicitly[Packer[V]].serialize(t._3, state)
    }
  }

  implicit val signatureSerializable: Packer[Signature] = new Packer[Signature] {
    def serialize(s: Signature, state: WriteState): Unit = ???
  }

  /**
   * Serializes a Class object, simply by storing the name of the class as a string. Primitives are given special
   * treatment.
   */
  implicit val classPacker: Packer[Class[_]] = new Packer[Class[_]] {
    def serialize(c: Class[_], state: WriteState): Unit =
      stringPacker.serialize(
        c.getName match {
          case "int"     => "I"
          case "long"    => "J"
          case "boolean" => "Z"
          case "char"    => "C"
          case "double"  => "D"
          case "float"   => "F"
          case "short"   => "S"
          case "byte"    => "B"
          case c         => "L" + c
        },
        state
      )
  }

  implicit val typeInfoPacker: Packer[TypeInfo[_]] = new Packer[TypeInfo[_]] {
    def serialize(ti: TypeInfo[_], state: WriteState): Unit = {
      implicitly[Packer[Seq[Class[_]]]].serialize(ti.classes, state)
      implicitly[Packer[Seq[Signature]]].serialize(ti.pureStructuralMethods, state)
      implicitly[Packer[Seq[(String, Class[_])]]].serialize(ti.primaryConstructorParams, state)
      implicitly[Packer[Seq[TypeInfo[_]]]].serialize(ti.typeParams, state)
    }
  }
}

object Unpacker {

  def apply[T: Unpacker](buf: ByteBuffer): T = {
    val state = new ReadState(buf)
    implicitly[Unpacker[T]].deserialize(state)
  }

  class ReadState(private val buf: ByteBuffer) {
    protected[Unpacker] val cache = new ListBuffer[String]()
    def next(): Byte = buf.get()
    def next(n: Int): Array[Byte] = {
      val a = new Array[Byte](n)
      buf.get(a)
      a
    }
  }

  /**
   * Packs sequences, much like the way strings are packed.
   */
  implicit def seqUnpacker[T: Unpacker]: Unpacker[Seq[T]] = new Unpacker[Seq[T]] {
    def deserialize(state: ReadState): Seq[T] = {
      Seq.fill(state.next() match {
        case -128 => (state.next() << 8) & 0xff + state.next() & 0xff
        case n    => n
      })(implicitly[Unpacker[T]].deserialize(state))
    }
  }

  implicit val intUnpacker: Unpacker[Int] = new Unpacker[Int] {
    def deserialize(state: ReadState): Int = (state.next() & 0xff) match {
      case 0xff =>
        ((state.next() & 0xff) << 24) +
          ((state.next() & 0xff) << 16) +
          ((state.next() & 0xff) << 8) +
          (state.next() & 0xff)
      case i => i
    }
  }

  /**
   * Unpacks strings. Strings are stored as an initial byte, normally representing the length of the string, followed by
   * the bytes of the string. If the length byte is -128, this indicates that the string is longer than 127 characters,
   * and the subsequent two bytes will represent the length, as a 16-bit integer. If the length byte is any other
   * negative number, it is considered a back-reference to a string which has been previously packed/unpacked.
   */
  implicit val stringUnpacker: Unpacker[String] = new Unpacker[String] {
    def deserialize(state: ReadState): String = state.next() match {
      case -128 =>
        val s = new String(state.next((state.next() << 8) & 0xff + state.next() & 0xff))
        state.cache += s
        s
      case n if n < 0 => state.cache(-n.toInt - 1)
      case n =>
        val s = new String(state.next(n))
        state.cache += s
        s
    }
  }

  implicit def tupleUnpacker[T: Unpacker, U: Unpacker]: Unpacker[(T, U)] = new Unpacker[(T, U)] {
    def deserialize(state: ReadState): (T, U) =
      (implicitly[Unpacker[T]].deserialize(state), implicitly[Unpacker[U]].deserialize(state))
  }

  implicit def tuple3Unpacker[T: Unpacker, U: Unpacker, V: Unpacker]: Unpacker[(T, U, V)] = new Unpacker[(T, U, V)] {
    def deserialize(state: ReadState): (T, U, V) =
      (
        implicitly[Unpacker[T]].deserialize(state),
        implicitly[Unpacker[U]].deserialize(state),
        implicitly[Unpacker[V]].deserialize(state))
  }

  implicit val signatureSerializable: Unpacker[Signature] = new Unpacker[Signature] {
    def deserialize(state: ReadState): Signature = {
      state.next(); null.asInstanceOf[Signature]
    }
  }

  /**
   * Serializes a Class object, simply by storing the name of the class as a string. Primitives are given special
   * treatment.
   */
  implicit val classUnpacker: Unpacker[Option[Class[_]]] = new Unpacker[Option[Class[_]]] {
    def deserialize(state: ReadState): Option[Class[_]] = stringUnpacker.deserialize(state) match {
      case "I" => Some(classOf[Int])
      case "J" => Some(classOf[Long])
      case "Z" => Some(classOf[Boolean])
      case "C" => Some(classOf[Char])
      case "D" => Some(classOf[Double])
      case "F" => Some(classOf[Float])
      case "S" => Some(classOf[Short])
      case "B" => Some(classOf[Byte])
      case c =>
        try {
          Some(Class.forName(c.substring(1)))
        } catch {
          // Handle the case that the serialized class is no longer defined.
          case _: ClassNotFoundException => None
        }
    }
  }

  implicit val typeInfoUnpacker: Unpacker[TypeInfo[_]] = new Unpacker[TypeInfo[_]] {
    def deserialize(state: ReadState): TypeInfo[_] = {
      TypeInfo(
        implicitly[Unpacker[Seq[Option[Class[_]]]]].deserialize(state).collect { case Some(cls) => cls },
        implicitly[Unpacker[Seq[Signature]]].deserialize(state),
        implicitly[Unpacker[Seq[(String, Option[Class[_]])]]]
          .deserialize(state)
          .collect { case (str, Some(cls)) => str -> cls },
        implicitly[Unpacker[Seq[TypeInfo[_]]]].deserialize(state)
      )
    }
  }
}
