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

import optimus.graph.NodeFuture
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
abstract class LNodeFunction0[R] extends LNodeFunction[R] with NodeFunction0[R] {
  override def apply$queued(): NodeFuture[R] = apply$newNode().enqueue
  def apply$newNode(): Node[R] = new AsNode0(this)
  // We override the key directly, because we don't need it to compute anything and can be null
  override def toNodeWith(key: PropertyNode[_]): Node[R] = new AsNode0(this)
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    throw new GraphInInvalidState("this shouldn't be called!")
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction1[T1, R] extends LNodeFunction[R] with NodeFunction1[T1, R] {
  def apply$queued(v1: T1): NodeFuture[R] = apply$newNode(v1).enqueue
  def apply$newNode(v1: T1): Node[R] = new AsNode1(this, v1)
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode1[T1, R](this, entity.asInstanceOf[T1])
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction2[T1, T2, R] extends LNodeFunction[R] with NodeFunction2[T1, T2, R] {
  def apply$queued(v1: T1, v2: T2): NodeFuture[R] = apply$newNode(v1, v2).enqueue
  def apply$newNode(v1: T1, v2: T2): Node[R] = new AsNode2(this, v1, v2)
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode2(this, entity.asInstanceOf[T1], args(0).asInstanceOf[T2])
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction3[T1, T2, T3, R] extends LNodeFunction[R] with NodeFunction3[T1, T2, T3, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3): NodeFuture[R] = apply$newNode(v1, v2, v3).enqueue
  def apply$newNode(v1: T1, v2: T2, v3: T3): Node[R] = new AsNode3(this, v1, v2, v3)
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode3(this, entity.asInstanceOf[T1], args(0).asInstanceOf[T2], args(1).asInstanceOf[T3])
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction4[T1, T2, T3, T4, R] extends LNodeFunction[R] with NodeFunction4[T1, T2, T3, T4, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4): NodeFuture[R] = apply$newNode(v1, v2, v3, v4).enqueue
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
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5): NodeFuture[R] =
    new AsNode5(this, v1, v2, v3, v4, v5).enqueue
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
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6): NodeFuture[R] =
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
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7): NodeFuture[R] =
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
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8): NodeFuture[R] =
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
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9): NodeFuture[R] =
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

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R]
    extends LNodeFunction[R]
    with NodeFunction10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9, v10: T10): NodeFuture[R] =
    new AsNode10(this, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode10(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R]
    extends LNodeFunction[R]
    with NodeFunction11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9, v10: T10, v11: T11)
      : NodeFuture[R] =
    new AsNode11(this, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode11(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10],
      args(9).asInstanceOf[T11]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R]
    extends LNodeFunction[R]
    with NodeFunction12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, R] {
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9, v10: T10, v11: T11, v12: T12)
      : NodeFuture[R] =
    new AsNode12(this, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode12(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10],
      args(9).asInstanceOf[T11],
      args(10).asInstanceOf[T12]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R]
    extends LNodeFunction[R]
    with NodeFunction13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, R] {
  def apply$queued(
      v1: T1,
      v2: T2,
      v3: T3,
      v4: T4,
      v5: T5,
      v6: T6,
      v7: T7,
      v8: T8,
      v9: T9,
      v10: T10,
      v11: T11,
      v12: T12,
      v13: T13): NodeFuture[R] =
    new AsNode13(this, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode13(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10],
      args(9).asInstanceOf[T11],
      args(10).asInstanceOf[T12],
      args(11).asInstanceOf[T13]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, R]
    extends LNodeFunction[R]
    with NodeFunction14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, R] {
  def apply$queued(
      v1: T1,
      v2: T2,
      v3: T3,
      v4: T4,
      v5: T5,
      v6: T6,
      v7: T7,
      v8: T8,
      v9: T9,
      v10: T10,
      v11: T11,
      v12: T12,
      v13: T13,
      v14: T14): NodeFuture[R] =
    new AsNode14(this, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode14(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10],
      args(9).asInstanceOf[T11],
      args(10).asInstanceOf[T12],
      args(11).asInstanceOf[T13],
      args(12).asInstanceOf[T14]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, R]
    extends LNodeFunction[R]
    with NodeFunction15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, R] {
  def apply$queued(
      v1: T1,
      v2: T2,
      v3: T3,
      v4: T4,
      v5: T5,
      v6: T6,
      v7: T7,
      v8: T8,
      v9: T9,
      v10: T10,
      v11: T11,
      v12: T12,
      v13: T13,
      v14: T14,
      v15: T15): NodeFuture[R] =
    new AsNode15(this, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode15(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10],
      args(9).asInstanceOf[T11],
      args(10).asInstanceOf[T12],
      args(11).asInstanceOf[T13],
      args(12).asInstanceOf[T14],
      args(13).asInstanceOf[T15]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, R]
    extends LNodeFunction[R]
    with NodeFunction16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, R] {
  def apply$queued(
      v1: T1,
      v2: T2,
      v3: T3,
      v4: T4,
      v5: T5,
      v6: T6,
      v7: T7,
      v8: T8,
      v9: T9,
      v10: T10,
      v11: T11,
      v12: T12,
      v13: T13,
      v14: T14,
      v15: T15,
      v16: T16): NodeFuture[R] =
    new AsNode16(this, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode16(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10],
      args(9).asInstanceOf[T11],
      args(10).asInstanceOf[T12],
      args(11).asInstanceOf[T13],
      args(12).asInstanceOf[T14],
      args(13).asInstanceOf[T15],
      args(14).asInstanceOf[T16]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R]
    extends LNodeFunction[R]
    with NodeFunction17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, R] {
  def apply$queued(
      v1: T1,
      v2: T2,
      v3: T3,
      v4: T4,
      v5: T5,
      v6: T6,
      v7: T7,
      v8: T8,
      v9: T9,
      v10: T10,
      v11: T11,
      v12: T12,
      v13: T13,
      v14: T14,
      v15: T15,
      v16: T16,
      v17: T17): NodeFuture[R] =
    new AsNode17(this, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode17(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10],
      args(9).asInstanceOf[T11],
      args(10).asInstanceOf[T12],
      args(11).asInstanceOf[T13],
      args(12).asInstanceOf[T14],
      args(13).asInstanceOf[T15],
      args(14).asInstanceOf[T16],
      args(15).asInstanceOf[T17]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, R]
    extends LNodeFunction[R]
    with NodeFunction18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, R] {
  def apply$queued(
      v1: T1,
      v2: T2,
      v3: T3,
      v4: T4,
      v5: T5,
      v6: T6,
      v7: T7,
      v8: T8,
      v9: T9,
      v10: T10,
      v11: T11,
      v12: T12,
      v13: T13,
      v14: T14,
      v15: T15,
      v16: T16,
      v17: T17,
      v18: T18): NodeFuture[R] =
    new AsNode18(this, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode18(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10],
      args(9).asInstanceOf[T11],
      args(10).asInstanceOf[T12],
      args(11).asInstanceOf[T13],
      args(12).asInstanceOf[T14],
      args(13).asInstanceOf[T15],
      args(14).asInstanceOf[T16],
      args(15).asInstanceOf[T17],
      args(16).asInstanceOf[T18]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, R]
    extends LNodeFunction[R]
    with NodeFunction19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, R] {
  def apply$queued(
      v1: T1,
      v2: T2,
      v3: T3,
      v4: T4,
      v5: T5,
      v6: T6,
      v7: T7,
      v8: T8,
      v9: T9,
      v10: T10,
      v11: T11,
      v12: T12,
      v13: T13,
      v14: T14,
      v15: T15,
      v16: T16,
      v17: T17,
      v18: T18,
      v19: T19): NodeFuture[R] =
    new AsNode19(this, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode19(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10],
      args(9).asInstanceOf[T11],
      args(10).asInstanceOf[T12],
      args(11).asInstanceOf[T13],
      args(12).asInstanceOf[T14],
      args(13).asInstanceOf[T15],
      args(14).asInstanceOf[T16],
      args(15).asInstanceOf[T17],
      args(16).asInstanceOf[T18],
      args(17).asInstanceOf[T19]
    )
}

//noinspection ScalaUnusedSymbol
abstract class LNodeFunction20[
    T1,
    T2,
    T3,
    T4,
    T5,
    T6,
    T7,
    T8,
    T9,
    T10,
    T11,
    T12,
    T13,
    T14,
    T15,
    T16,
    T17,
    T18,
    T19,
    T20,
    R]
    extends LNodeFunction[R]
    with NodeFunction20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, R] {
  def apply$queued(
      v1: T1,
      v2: T2,
      v3: T3,
      v4: T4,
      v5: T5,
      v6: T6,
      v7: T7,
      v8: T8,
      v9: T9,
      v10: T10,
      v11: T11,
      v12: T12,
      v13: T13,
      v14: T14,
      v15: T15,
      v16: T16,
      v17: T17,
      v18: T18,
      v19: T19,
      v20: T20): NodeFuture[R] =
    new AsNode20(
      this,
      v1,
      v2,
      v3,
      v4,
      v5,
      v6,
      v7,
      v8,
      v9,
      v10,
      v11,
      v12,
      v13,
      v14,
      v15,
      v16,
      v17,
      v18,
      v19,
      v20).enqueue
  override def toNodeWith(entity: Entity, args: Array[AnyRef]): Node[R] =
    new AsNode20(
      this,
      entity.asInstanceOf[T1],
      args(0).asInstanceOf[T2],
      args(1).asInstanceOf[T3],
      args(2).asInstanceOf[T4],
      args(3).asInstanceOf[T5],
      args(4).asInstanceOf[T6],
      args(5).asInstanceOf[T7],
      args(6).asInstanceOf[T8],
      args(7).asInstanceOf[T9],
      args(8).asInstanceOf[T10],
      args(9).asInstanceOf[T11],
      args(10).asInstanceOf[T12],
      args(11).asInstanceOf[T13],
      args(12).asInstanceOf[T14],
      args(13).asInstanceOf[T15],
      args(14).asInstanceOf[T16],
      args(15).asInstanceOf[T17],
      args(16).asInstanceOf[T18],
      args(17).asInstanceOf[T19],
      args(18).asInstanceOf[T20]
    )
}
