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
package optimus.graph.loom

import optimus.graph.{GraphInInvalidState, Node, PropertyNode}
import optimus.platform._
import optimus.platform.storable.Entity
import optimus.graph.loom.LoomConfig._

import java.io.Serial

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction[R] extends LNodeClsID {
  def toNodeWith(key: PropertyNode[_]): Node[R] = toNodeWith(key.entity, key.args)
  def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R]

  override def hashCode(): Int = getClass.hashCode() ^ argsHashCode()
  def argsHashCode(): Int = 0
  override def equals(obj: Any): Boolean = (obj != null) && (this.getClass eq obj.getClass)
  def getProfileId: Int = LPropertyDescriptor.get(_clsID()).profileID

  @Serial protected def writeReplace(): AnyRef = {
    // we can lookup the fields here cause we know they are stable!
    val args = getClass.getDeclaredFields.map(_.get(this))
    val desc = LPropertyDescriptor.get(_clsID())
    new SerializedNode(desc.className, desc.methodName, desc.localID, args)
  }
}

class SerializedNode(val className: String, val methodName: String, val localID: Int, val args: Array[AnyRef])
    extends Serializable {

  @Serial protected def readResolve(): Any = {
    val cls = Class.forName(className); // ...and forcing registration
    val deserializeMethod = cls.getDeclaredMethod(DESERIALIZE, DESERIALIZE_MT.parameterArray(): _*)
    deserializeMethod.setAccessible(true)
    val nodeFuncClass = deserializeMethod.invoke(null, Int.box(localID)).asInstanceOf[Class[_]]
    // we generate the class, so we can guarantee there is only one ctor!
    val ctor = nodeFuncClass.getConstructors.head
    ctor.newInstance(args: _*)
  }

}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction0[R] extends LNodeFunction[R] with NodeFunction0NN[R] {
  override def apply$queued(): Node[R] = apply$newNode().enqueue
  override def apply$newNode(): Node[R] = new AsNode0(this)
  // We override the key directly, because we don't need it to compute anything and can be null
  override def toNodeWith(key: PropertyNode[_]): Node[R] = new AsNode0(this)
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    throw new GraphInInvalidState("this shouldn't be called!")
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction1[T1, R] extends LNodeFunction[R] with NodeFunction1[T1, R] {
  def apply$queued(v1: T1): Node[R] = apply$newNode(v1).enqueue
  def apply$newNode(v1: T1): Node[R] = new AsNode1(this, v1)
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode1[T1, R](this, entity.asInstanceOf[T1])
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction2[T1, T2, R] extends LNodeFunction[R] with NodeFunction2[T1, T2, R] {
  def apply$queued(v1: T1, v2: T2): Node[R] = apply$newNode(v1, v2).enqueue
  def apply$newNode(v1: T1, v2: T2): Node[R] = new AsNode2(this, v1, v2)
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode2(this, entity.asInstanceOf[T1], args(0).asInstanceOf[T2])
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction3[T1, T2, T3, R] extends LNodeFunction[R] with NodeFunction3[T1, T2, T3, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3): Node[R] = apply$newNode(v1, v2, v3).enqueue
  def apply$newNode(v1: T1, v2: T2, v3: T3): Node[R] = new AsNode3(this, v1, v2, v3)
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode3(this, entity.asInstanceOf[T1], args(0).asInstanceOf[T2], args(1).asInstanceOf[T3])
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction4[T1, T2, T3, T4, R] extends LNodeFunction[R] with NodeFunction4[T1, T2, T3, T4, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4): Node[R] = apply$newNode(v1, v2, v3, v4).enqueue
  def apply$newNode(v1: T1, v2: T2, v3: T3, v4: T4): Node[R] = new AsNode4(this, v1, v2, v3, v4)
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode4(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4])
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction5[T1, T2, T3, T4, T5, R]
    extends LNodeFunction[R]
    with NodeFunction5[T1, T2, T3, T4, T5, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5): Node[R] = new AsNode5(this, v1, v2, v3, v4, v5).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode5(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5])
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction6[T1, T2, T3, T4, T5, T6, R]
    extends LNodeFunction[R]
    with NodeFunction6[T1, T2, T3, T4, T5, T6, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6): Node[R] =
    new AsNode6(this, v1, v2, v3, v4, v5, v6).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode6(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction7[T1, T2, T3, T4, T5, T6, T7, R]
    extends LNodeFunction[R]
    with NodeFunction7[T1, T2, T3, T4, T5, T6, T7, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7): Node[R] =
    new AsNode7(this, v1, v2, v3, v4, v5, v6, v7).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode7(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R]
    extends LNodeFunction[R]
    with NodeFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8): Node[R] =
    new AsNode8(this, v1, v2, v3, v4, v5, v6, v7, v8).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode8(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]
    extends LNodeFunction[R]
    with NodeFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9): Node[R] =
    new AsNode9(this, v1, v2, v3, v4, v5, v6, v7, v8, v9).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode9(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9]
    )
}
