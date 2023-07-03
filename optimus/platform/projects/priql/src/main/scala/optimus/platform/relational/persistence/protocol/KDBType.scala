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
package optimus.platform.relational.persistence.protocol

private[persistence] object KDBType extends Enumeration {
  type KDBType = Value

  val Bool = Value(-1)
  val Byte = Value(-4)
  val Short = Value(-5)
  val Int = Value(-6)
  val Long = Value(-7)
  val Float = Value(-8)
  val Double = Value(-9)
  val Char = Value(-10)
  val String = Value(-11)
  val TimeStamp = Value(-12)
  val Month = Value(-13)
  val Date = Value(-14)
  val DateTime = Value(-15)
  val KTimeSpan = Value(-16)
  val Minute = Value(-17)
  val Second = Value(-18)
  val TimeSpan = Value(-19)
  val GeneralList = Value(0)
  val BoolList = Value(1)
  val ByteList = Value(4)
  val ShortList = Value(5)
  val IntList = Value(6)
  val LongList = Value(7)
  val FloatList = Value(8)
  val DoubleList = Value(9)
  val CharList = Value(10)
  val StringList = Value(11)
  val TimeStampList = Value(12)
  val MonthList = Value(13)
  val DateList = Value(14)
  val DateTimeList = Value(15)
  val KTimeSpanList = Value(16)
  val MinuteList = Value(17)
  val SecondList = Value(18)
  val TimeSpanList = Value(19)
  val Flip = Value(98)
  val Dict = Value(99)
  val Null = Value(101)
  val Object = Value(1000)
}
