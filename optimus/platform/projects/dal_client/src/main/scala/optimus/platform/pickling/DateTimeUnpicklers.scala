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

import optimus.platform.annotations.nodeSync

import java.time.Duration
import java.time.Instant
import java.time._

private[optimus] object DateTimeUnpicklers {
  object LocalDateUnpickler extends Unpickler[LocalDate] {
    @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream) = pickled match {
      case l: LocalDate => l
      case s: String    => LocalDate.parse(s)
      case o            => throw new UnexpectedPickledTypeException(implicitly[Manifest[LocalDate]], o.getClass)
    }
  }

  object ZoneIdUnpickler extends Unpickler[ZoneId] {
    @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream) = pickled match {
      case z: ZoneId => z
      case o         => throw new UnexpectedPickledTypeException(implicitly[Manifest[ZoneId]], o.getClass)
    }
  }

  class InstantUnpickler extends Unpickler[Instant] {
    @nodeSync override def unpickle(pickled: Any, ctxt: PickledInputStream): Instant = pickled match {
      case buf: collection.Seq[_] =>
        val temp = buf(0)
        val seconds = LongUnpickler.unpickle(temp, ctxt)
        val nanos = buf(1).asInstanceOf[Int]
        Instant.ofEpochSecond(seconds, nanos)
    }
  }

  class DurationUnpickler extends Unpickler[Duration] {
    @nodeSync override def unpickle(pickled: Any, ctxt: PickledInputStream): Duration = pickled match {
      case buf: collection.Seq[_] =>
        val temp = buf(0)
        val seconds = LongUnpickler.unpickle(temp, ctxt)
        val nanos = buf(1).asInstanceOf[Int]
        Duration.ofSeconds(seconds, nanos)
    }
  }

  class MonthUnpickler extends Unpickler[YearMonth] {
    @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream) = pickled match {
      case buf: collection.Seq[_] =>
        val year = buf(0).asInstanceOf[Int]
        val month = buf(1).asInstanceOf[Int]
        YearMonth.of(year, month)
    }
  }

  class YearUnpickler extends Unpickler[Year] {
    @nodeSync def unpickle(pickled: Any, ctxt: PickledInputStream) = pickled match {
      case i: Int => Year.of(i)
      case _      => throw new UnexpectedPickledTypeException(Manifest.Int, pickled.getClass)
    }
  }
}
