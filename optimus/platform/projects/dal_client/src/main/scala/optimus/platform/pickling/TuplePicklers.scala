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

trait TuplePicklers {
  def tuple2pickler[T1, T2](picklerT1: Pickler[T1], picklerT2: Pickler[T2]): Pickler[(T1, T2)] =
    new Tuple2Pickler[T1, T2](picklerT1, picklerT2)
  def tuple3pickler[T1, T2, T3](
      picklerT1: Pickler[T1],
      picklerT2: Pickler[T2],
      picklerT3: Pickler[T3]): Pickler[(T1, T2, T3)] =
    new Tuple3Pickler[T1, T2, T3](picklerT1, picklerT2, picklerT3)
  def tuple4pickler[T1, T2, T3, T4](
      picklerT1: Pickler[T1],
      picklerT2: Pickler[T2],
      picklerT3: Pickler[T3],
      picklerT4: Pickler[T4]): Pickler[(T1, T2, T3, T4)] =
    new Tuple4Pickler[T1, T2, T3, T4](picklerT1, picklerT2, picklerT3, picklerT4)
  def tuple5pickler[T1, T2, T3, T4, T5](
      picklerT1: Pickler[T1],
      picklerT2: Pickler[T2],
      picklerT3: Pickler[T3],
      picklerT4: Pickler[T4],
      picklerT5: Pickler[T5]): Pickler[(T1, T2, T3, T4, T5)] =
    new Tuple5Pickler[T1, T2, T3, T4, T5](picklerT1, picklerT2, picklerT3, picklerT4, picklerT5)
  def tuple6pickler[T1, T2, T3, T4, T5, T6](
      picklerT1: Pickler[T1],
      picklerT2: Pickler[T2],
      picklerT3: Pickler[T3],
      picklerT4: Pickler[T4],
      picklerT5: Pickler[T5],
      picklerT6: Pickler[T6]): Pickler[(T1, T2, T3, T4, T5, T6)] =
    new Tuple6Pickler[T1, T2, T3, T4, T5, T6](picklerT1, picklerT2, picklerT3, picklerT4, picklerT5, picklerT6)
  def tuple7pickler[T1, T2, T3, T4, T5, T6, T7](
      picklerT1: Pickler[T1],
      picklerT2: Pickler[T2],
      picklerT3: Pickler[T3],
      picklerT4: Pickler[T4],
      picklerT5: Pickler[T5],
      picklerT6: Pickler[T6],
      picklerT7: Pickler[T7]): Pickler[(T1, T2, T3, T4, T5, T6, T7)] =
    new Tuple7Pickler[T1, T2, T3, T4, T5, T6, T7](
      picklerT1,
      picklerT2,
      picklerT3,
      picklerT4,
      picklerT5,
      picklerT6,
      picklerT7)
  def tuple8pickler[T1, T2, T3, T4, T5, T6, T7, T8](
      picklerT1: Pickler[T1],
      picklerT2: Pickler[T2],
      picklerT3: Pickler[T3],
      picklerT4: Pickler[T4],
      picklerT5: Pickler[T5],
      picklerT6: Pickler[T6],
      picklerT7: Pickler[T7],
      picklerT8: Pickler[T8]): Pickler[(T1, T2, T3, T4, T5, T6, T7, T8)] =
    new Tuple8Pickler[T1, T2, T3, T4, T5, T6, T7, T8](
      picklerT1,
      picklerT2,
      picklerT3,
      picklerT4,
      picklerT5,
      picklerT6,
      picklerT7,
      picklerT8)
  def tuple9pickler[T1, T2, T3, T4, T5, T6, T7, T8, T9](
      picklerT1: Pickler[T1],
      picklerT2: Pickler[T2],
      picklerT3: Pickler[T3],
      picklerT4: Pickler[T4],
      picklerT5: Pickler[T5],
      picklerT6: Pickler[T6],
      picklerT7: Pickler[T7],
      picklerT8: Pickler[T8],
      picklerT9: Pickler[T9]): Pickler[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] =
    new Tuple9Pickler[T1, T2, T3, T4, T5, T6, T7, T8, T9](
      picklerT1,
      picklerT2,
      picklerT3,
      picklerT4,
      picklerT5,
      picklerT6,
      picklerT7,
      picklerT8,
      picklerT9)
  def tuple10pickler[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](
      picklerT1: Pickler[T1],
      picklerT2: Pickler[T2],
      picklerT3: Pickler[T3],
      picklerT4: Pickler[T4],
      picklerT5: Pickler[T5],
      picklerT6: Pickler[T6],
      picklerT7: Pickler[T7],
      picklerT8: Pickler[T8],
      picklerT9: Pickler[T9],
      picklerT10: Pickler[T10]): Pickler[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] =
    new Tuple10Pickler[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](
      picklerT1,
      picklerT2,
      picklerT3,
      picklerT4,
      picklerT5,
      picklerT6,
      picklerT7,
      picklerT8,
      picklerT9,
      picklerT10)
}

class Tuple2Pickler[T1, T2](picklerT1: Pickler[T1], picklerT2: Pickler[T2]) extends Pickler[(T1, T2)] {
  def pickle(t: (T1, T2), visitor: PickledOutputStream): Unit = {
    if (t eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    picklerT1.pickle(t._1, visitor)
    picklerT2.pickle(t._2, visitor)
    visitor.writeEndArray()
  }
}

class Tuple3Pickler[T1, T2, T3](picklerT1: Pickler[T1], picklerT2: Pickler[T2], picklerT3: Pickler[T3])
    extends Pickler[(T1, T2, T3)] {
  def pickle(t: (T1, T2, T3), visitor: PickledOutputStream): Unit = {
    if (t eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    picklerT1.pickle(t._1, visitor)
    picklerT2.pickle(t._2, visitor)
    picklerT3.pickle(t._3, visitor)
    visitor.writeEndArray()
  }
}

class Tuple4Pickler[T1, T2, T3, T4](
    picklerT1: Pickler[T1],
    picklerT2: Pickler[T2],
    picklerT3: Pickler[T3],
    picklerT4: Pickler[T4])
    extends Pickler[(T1, T2, T3, T4)] {
  def pickle(t: (T1, T2, T3, T4), visitor: PickledOutputStream): Unit = {
    if (t eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    picklerT1.pickle(t._1, visitor)
    picklerT2.pickle(t._2, visitor)
    picklerT3.pickle(t._3, visitor)
    picklerT4.pickle(t._4, visitor)
    visitor.writeEndArray()
  }
}

class Tuple5Pickler[T1, T2, T3, T4, T5](
    picklerT1: Pickler[T1],
    picklerT2: Pickler[T2],
    picklerT3: Pickler[T3],
    picklerT4: Pickler[T4],
    picklerT5: Pickler[T5])
    extends Pickler[(T1, T2, T3, T4, T5)] {
  def pickle(t: (T1, T2, T3, T4, T5), visitor: PickledOutputStream): Unit = {
    if (t eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    picklerT1.pickle(t._1, visitor)
    picklerT2.pickle(t._2, visitor)
    picklerT3.pickle(t._3, visitor)
    picklerT4.pickle(t._4, visitor)
    picklerT5.pickle(t._5, visitor)
    visitor.writeEndArray()
  }
}

class Tuple6Pickler[T1, T2, T3, T4, T5, T6](
    picklerT1: Pickler[T1],
    picklerT2: Pickler[T2],
    picklerT3: Pickler[T3],
    picklerT4: Pickler[T4],
    picklerT5: Pickler[T5],
    picklerT6: Pickler[T6])
    extends Pickler[(T1, T2, T3, T4, T5, T6)] {
  def pickle(t: (T1, T2, T3, T4, T5, T6), visitor: PickledOutputStream): Unit = {
    if (t eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    picklerT1.pickle(t._1, visitor)
    picklerT2.pickle(t._2, visitor)
    picklerT3.pickle(t._3, visitor)
    picklerT4.pickle(t._4, visitor)
    picklerT5.pickle(t._5, visitor)
    picklerT6.pickle(t._6, visitor)
    visitor.writeEndArray()
  }
}

class Tuple7Pickler[T1, T2, T3, T4, T5, T6, T7](
    picklerT1: Pickler[T1],
    picklerT2: Pickler[T2],
    picklerT3: Pickler[T3],
    picklerT4: Pickler[T4],
    picklerT5: Pickler[T5],
    picklerT6: Pickler[T6],
    picklerT7: Pickler[T7])
    extends Pickler[(T1, T2, T3, T4, T5, T6, T7)] {
  def pickle(t: (T1, T2, T3, T4, T5, T6, T7), visitor: PickledOutputStream): Unit = {
    if (t eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    picklerT1.pickle(t._1, visitor)
    picklerT2.pickle(t._2, visitor)
    picklerT3.pickle(t._3, visitor)
    picklerT4.pickle(t._4, visitor)
    picklerT5.pickle(t._5, visitor)
    picklerT6.pickle(t._6, visitor)
    picklerT7.pickle(t._7, visitor)
    visitor.writeEndArray()
  }
}

class Tuple8Pickler[T1, T2, T3, T4, T5, T6, T7, T8](
    picklerT1: Pickler[T1],
    picklerT2: Pickler[T2],
    picklerT3: Pickler[T3],
    picklerT4: Pickler[T4],
    picklerT5: Pickler[T5],
    picklerT6: Pickler[T6],
    picklerT7: Pickler[T7],
    picklerT8: Pickler[T8])
    extends Pickler[(T1, T2, T3, T4, T5, T6, T7, T8)] {
  def pickle(t: (T1, T2, T3, T4, T5, T6, T7, T8), visitor: PickledOutputStream): Unit = {
    if (t eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    picklerT1.pickle(t._1, visitor)
    picklerT2.pickle(t._2, visitor)
    picklerT3.pickle(t._3, visitor)
    picklerT4.pickle(t._4, visitor)
    picklerT5.pickle(t._5, visitor)
    picklerT6.pickle(t._6, visitor)
    picklerT7.pickle(t._7, visitor)
    picklerT8.pickle(t._8, visitor)
    visitor.writeEndArray()
  }
}

class Tuple9Pickler[T1, T2, T3, T4, T5, T6, T7, T8, T9](
    picklerT1: Pickler[T1],
    picklerT2: Pickler[T2],
    picklerT3: Pickler[T3],
    picklerT4: Pickler[T4],
    picklerT5: Pickler[T5],
    picklerT6: Pickler[T6],
    picklerT7: Pickler[T7],
    picklerT8: Pickler[T8],
    picklerT9: Pickler[T9])
    extends Pickler[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] {
  def pickle(t: (T1, T2, T3, T4, T5, T6, T7, T8, T9), visitor: PickledOutputStream): Unit = {
    if (t eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    picklerT1.pickle(t._1, visitor)
    picklerT2.pickle(t._2, visitor)
    picklerT3.pickle(t._3, visitor)
    picklerT4.pickle(t._4, visitor)
    picklerT5.pickle(t._5, visitor)
    picklerT6.pickle(t._6, visitor)
    picklerT7.pickle(t._7, visitor)
    picklerT8.pickle(t._8, visitor)
    picklerT9.pickle(t._9, visitor)
    visitor.writeEndArray()
  }
}

class Tuple10Pickler[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](
    picklerT1: Pickler[T1],
    picklerT2: Pickler[T2],
    picklerT3: Pickler[T3],
    picklerT4: Pickler[T4],
    picklerT5: Pickler[T5],
    picklerT6: Pickler[T6],
    picklerT7: Pickler[T7],
    picklerT8: Pickler[T8],
    picklerT9: Pickler[T9],
    picklerT10: Pickler[T10])
    extends Pickler[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] {
  def pickle(t: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10), visitor: PickledOutputStream): Unit = {
    if (t eq null)
      throw new PicklingException("Cannot pickle null values")
    visitor.writeStartArray()
    picklerT1.pickle(t._1, visitor)
    picklerT2.pickle(t._2, visitor)
    picklerT3.pickle(t._3, visitor)
    picklerT4.pickle(t._4, visitor)
    picklerT5.pickle(t._5, visitor)
    picklerT6.pickle(t._6, visitor)
    picklerT7.pickle(t._7, visitor)
    picklerT8.pickle(t._8, visitor)
    picklerT9.pickle(t._9, visitor)
    picklerT10.pickle(t._10, visitor)
    visitor.writeEndArray()
  }
}
