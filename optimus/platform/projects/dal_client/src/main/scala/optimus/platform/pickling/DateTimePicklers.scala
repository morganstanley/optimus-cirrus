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

import java.time.Duration
import java.time.Instant
import java.time._

private object DateTimePicklers {
  class InstantPickler extends Pickler[Instant] {
    override def pickle(instant: Instant, visitor: PickledOutputStream): Unit = {
      visitor.writeStartArray()
      visitor.writeLong(instant.getEpochSecond())
      visitor.writeInt(instant.getNano())
      visitor.writeEndArray()
    }
  }

  class DurationPickler extends Pickler[Duration] {
    override def pickle(duration: Duration, visitor: PickledOutputStream) = {
      visitor.writeStartArray()
      visitor.writeLong(duration.getSeconds())
      visitor.writeInt(duration.getNano())
      visitor.writeEndArray()
    }
  }

  class MonthPickler extends Pickler[YearMonth] {
    override def pickle(instant: YearMonth, visitor: PickledOutputStream) = {
      visitor.writeStartArray()
      visitor.writeInt(instant.getYear())
      visitor.writeInt(instant.getMonthValue())
      visitor.writeEndArray()
    }
  }

  class YearPickler extends Pickler[Year] {
    override def pickle(year: Year, visitor: PickledOutputStream): Unit = {
      visitor.writeInt(year.getValue)
    }
  }
}
