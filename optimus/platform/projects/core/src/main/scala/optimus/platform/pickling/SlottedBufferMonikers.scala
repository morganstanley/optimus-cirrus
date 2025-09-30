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

import java.io.Externalizable
import java.io.ObjectInput
import java.io.ObjectOutput

@SerialVersionUID(1L)
class ShapeMoniker(var shape: Shape) extends Externalizable {
  // For de-serialization
  def this() = this(null)

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(shape.signature)
    out.writeBoolean(shape.hasTag)
    if (shape.hasTag) out.writeObject(shape.tag)
    var i = 0
    while (i < shape.names.length) {
      out.writeObject(shape.names(i))
      i += 1
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    val signature = in.readObject().asInstanceOf[String]
    val hasTag = in.readBoolean()
    val tag = if (hasTag) in.readObject().asInstanceOf[String] else Shape.NoTag
    val classes = Shape.classesFromSignature(signature)
    val names = new Array[String](classes.length)
    var i = 0
    while (i < names.length) {
      names(i) = in.readObject().asInstanceOf[String]
      i += 1
    }
    shape = Shape(names, classes, tag)
  }

  // noinspection ScalaUnusedSymbol
  def readResolve(): AnyRef = shape
}

@SerialVersionUID(1L)
class SlottedBufferAsSeqMoniker(var sb: SlottedBufferAsSeq) extends Externalizable {
  // For de-serialization
  def this() = this(null)

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(sb.signature)
    sb.write(out)
  }

  override def readExternal(in: ObjectInput): Unit = {
    val signature = in.readObject().asInstanceOf[String]
    val classes = Shape.classesFromSignature(signature)
    val shape = Shape(classes)
    sb = shape.readInstanceAsSeq(in)
  }

  // noinspection ScalaUnusedSymbol
  def readResolve(): AnyRef = sb
}

@SerialVersionUID(1L)
class SlottedBufferAsMapMoniker(var sb: SlottedBufferAsMap) extends Externalizable {
  // For de-serialization
  def this() = this(null)

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(sb.shape)
    sb.write(out)
  }

  override def readExternal(in: ObjectInput): Unit = {
    val shape = in.readObject().asInstanceOf[Shape]
    sb = shape.readInstanceAsMap(in)
  }

  // noinspection ScalaUnusedSymbol
  def readResolve(): AnyRef = sb
}
