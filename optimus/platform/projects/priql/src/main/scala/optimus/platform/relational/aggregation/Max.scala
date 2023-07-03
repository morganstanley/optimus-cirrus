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
package optimus.platform.relational.aggregation

import optimus.platform.relational.RelationalException
import optimus.platform.relational.tree.typeInfo
import optimus.platform.relational.tree.TypeInfo

/**
 * Max implementation
 */
trait Max[T] extends Aggregator[T] {
  type Result = T
  def default: Result = throw new RelationalException("Sequence contains no elements")
}

object Max {
  trait IntMax extends Max[Int] {
    type Seed = (Array[Int], Array[Boolean])
    val resultType = TypeInfo.INT
    def seed: Seed = (Array(0), Array(false))
    def accumulate(v: Int, s: Seed, state: LoopState): Seed = {
      if (s._2(0) == false || s._1(0) < v)
        s._1(0) = v
      s._2(0) = true
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      if (s1._2(0) && s2._2(0))
        s1._1(0) = Math.max(s1._1(0), s2._1(0))
      else if (s2._2(0))
        s1._1(0) = s2._1(0)
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1(0)) else None
    override def evaluateAll(values: Iterable[Int]): Option[Int] = {
      if (values.isEmpty) None
      else {
        val arr = values.toArray
        val length = arr.length
        var max = arr(0)
        var idx = 1
        while (idx < length) {
          max = math.max(max, arr(idx))
          idx += 1
        }
        Some(max)
      }
    }
  }
  implicit object Int extends IntMax

  trait LongMax extends Max[Long] {
    type Seed = (Array[Long], Array[Boolean])
    val resultType = TypeInfo.LONG
    def seed: Seed = (Array(0L), Array(false))
    def accumulate(v: Long, s: Seed, state: LoopState): Seed = {
      if (s._2(0) == false || s._1(0) < v)
        s._1(0) = v
      s._2(0) = true
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      if (s1._2(0) && s2._2(0))
        s1._1(0) = Math.max(s1._1(0), s2._1(0))
      else if (s2._2(0))
        s1._1(0) = s2._1(0)
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1(0)) else None
    override def evaluateAll(values: Iterable[Long]): Option[Long] = {
      if (values.isEmpty) None
      else {
        val arr = values.toArray
        val length = arr.length
        var max = arr(0)
        var idx = 1
        while (idx < length) {
          max = math.max(max, arr(idx))
          idx += 1
        }
        Some(max)
      }
    }
  }
  implicit object Long extends LongMax

  trait DoubleMax extends Max[Double] {
    type Seed = (Array[Double], Array[Boolean])
    val resultType = TypeInfo.DOUBLE
    def seed: Seed = (Array(0.0), Array(false))
    def accumulate(v: Double, s: Seed, state: LoopState): Seed = {
      if (s._2(0) == false || s._1(0) < v)
        s._1(0) = v
      s._2(0) = true
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      if (s1._2(0) && s2._2(0))
        s1._1(0) = Math.max(s1._1(0), s2._1(0))
      else if (s2._2(0))
        s1._1(0) = s2._1(0)
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1(0)) else None
    override def evaluateAll(values: Iterable[Double]): Option[Double] = {
      if (values.isEmpty) None
      else {
        val arr = values.toArray
        val length = arr.length
        var max = arr(0)
        var idx = 1
        while (idx < length) {
          max = math.max(max, arr(idx))
          idx += 1
        }
        Some(max)
      }
    }
  }
  implicit object Double extends DoubleMax

  trait FloatMax extends Max[Float] {
    type Seed = (Array[Float], Array[Boolean])
    val resultType = TypeInfo.FLOAT
    def seed: Seed = (Array(0f), Array(false))
    def accumulate(v: Float, s: Seed, state: LoopState): Seed = {
      if (s._2(0) == false || s._1(0) < v)
        s._1(0) = v
      s._2(0) = true
      s
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      if (s1._2(0) && s2._2(0))
        s1._1(0) = Math.max(s1._1(0), s2._1(0))
      else if (s2._2(0))
        s1._1(0) = s2._1(0)
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1(0)) else None
    override def evaluateAll(values: Iterable[Float]): Option[Float] = {
      if (values.isEmpty) None
      else {
        val arr = values.toArray
        val length = arr.length
        var max = arr(0)
        var idx = 1
        while (idx < length) {
          max = math.max(max, arr(idx))
          idx += 1
        }
        Some(max)
      }
    }
  }
  implicit object Float extends FloatMax

  class OrderingMax[T: TypeInfo](val ord: Ordering[T]) extends Max[T] {
    import Aggregator.ObjectRef

    type Seed = (ObjectRef[T], Array[Boolean])
    val resultType = typeInfo[T]
    def seed: Seed = (new ObjectRef[T](null.asInstanceOf[T]), Array(false))
    def accumulate(v: T, s: Seed, state: LoopState): Seed = {
      if (v == null || v == None) s
      else {
        if (s._2(0) == false || ord.compare(v, s._1.ref) > 0)
          s._1.ref = v
        s._2(0) = true
        s
      }
    }
    def combine(s1: Seed, s2: Seed, state: LoopState): Seed = {
      if (s1._2(0) && s2._2(0)) {
        if (ord.compare(s2._1.ref, s1._1.ref) > 0)
          s1._1.ref = s2._1.ref
      } else if (s2._2(0)) {
        s1._1.ref = s2._1.ref
      }
      s1._2(0) |= s2._2(0)
      s1
    }
    def result(s: Seed): Option[Result] = if (s._2(0)) Some(s._1.ref) else None
    override def default: Result = {
      if (resultType.clazz == classOf[Option[_]]) None.asInstanceOf[Result]
      else if (!resultType.clazz.isPrimitive()) null.asInstanceOf[Result]
      else super.default
    }
  }
  implicit def Ordering[T: TypeInfo](implicit ord: Ordering[T]): Max[T] = {
    new OrderingMax(ord)
  }
}
