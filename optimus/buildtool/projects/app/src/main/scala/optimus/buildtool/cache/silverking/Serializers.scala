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
package optimus.buildtool.cache.silverking

import java.nio.ByteBuffer

import com.ms.silverking.cloud.dht.client.serialization.BufferSerDes
import com.ms.silverking.cloud.dht.client.serialization.ByteArraySerDes

private[silverking] class DirectByteBufferSerDes extends BufferSerDes[ByteBuffer] {

  /** Note: This is a shallow copy of the input ByteBuffer */
  override def serializeToBuffer(obj: ByteBuffer): ByteBuffer = obj.duplicate()
  override def serializeToBuffer(obj: ByteBuffer, buffer: ByteBuffer): Unit = buffer.put(obj)
  override def estimateSerializedSize(obj: ByteBuffer): Int = obj.remaining
  override def emptyObject(): ByteBuffer = ByteBuffer.allocate(0)
  override def deserialize(bytes: Array[ByteBuffer]): ByteBuffer = {
    val totalRemaining = bytes.map(_.remaining).sum
    val obj = ByteBuffer.allocateDirect(totalRemaining)
    bytes.foreach(obj.put)
    obj.flip()
    obj
  }
  override def deserialize(bytes: ByteBuffer): ByteBuffer = deserialize(Array(bytes))
}

private[silverking] trait SimpleSerDes[A] extends BufferSerDes[A] {
  override def serializeToBuffer(a: A): ByteBuffer = ByteBuffer.wrap(toBytes(a))
  override def serializeToBuffer(a: A, byteBuffer: ByteBuffer): Unit = byteBuffer.put(toBytes(a))
  protected def toBytes(a: A): Array[Byte]
  override def deserialize(byteBuffers: Array[ByteBuffer]): A =
    fromBytes(ByteArraySerDes.deserializeBuffers(byteBuffers))
  override def deserialize(byteBuffer: ByteBuffer): A = deserialize(Array(byteBuffer))
  protected def fromBytes(bytes: Array[Byte]): A
}

import spray.json._
// Note: If you don't override emptyObject, then you won't be able to invalidate instances of A as a value.
// `null` is not a valid object for storage in SK.
private[silverking] class JsonSerDes[A: JsonFormat](override val emptyObject: A = null) extends SimpleSerDes[A] {
  import java.nio.charset.StandardCharsets.UTF_8

  protected def toBytes(a: A): Array[Byte] = {
    val str = a.toJson.prettyPrint
    str.getBytes(UTF_8)
  }

  protected def fromBytes(bytes: Array[Byte]): A = new String(bytes, UTF_8).parseJson.convertTo[A]

  override def estimateSerializedSize(t: A): Int = 0
}
