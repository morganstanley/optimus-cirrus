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
package optimus.utils

final case class MemSize private (bytes: Long, unit: MemUnit) extends Ordered[MemSize] {
  def in(memUnit: MemUnit): MemSize = copy(unit = memUnit)

  def value: Long = bytes / unit.bytes

  override def compare(that: MemSize): Int = bytes.compareTo(that.bytes)

  override def toString = s"$value ${unit.name}"

  def to(toUnit: MemUnit): MemSize = this.copy(unit = toUnit)

}

object MemSize {
  def of(value: Long, unit: MemUnit = MemUnit.Bytes): MemSize = {
    MemSize(value * unit.bytes, unit)
  }

  def of(unitString: String): MemSize = {
    val regex = """(\d+)\s*(\w+)""".r
    unitString match {
      case regex(size, unit) =>
        unit.toUpperCase() match {
          case MemUnit.Bytes.name    => of(size.toLong, MemUnit.Bytes)
          case MemUnit.KB.name | "K" => of(size.toLong, MemUnit.KB)
          case MemUnit.MB.name | "M" => of(size.toLong, MemUnit.MB)
          case MemUnit.GB.name | "G" => of(size.toLong, MemUnit.GB)
        }
    }
  }
}

/**
 * Memory units
 */
final case class MemUnit(bytes: Long, name: String)

object MemUnit {
  val Bytes = MemUnit(1, "B")
  val KB = MemUnit(1024, "KB")
  val MB = MemUnit(1024 * 1024, "MB")
  val GB = MemUnit(1024 * 1024 * 1024, "GB")
}
