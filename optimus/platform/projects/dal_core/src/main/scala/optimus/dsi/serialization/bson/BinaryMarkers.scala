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
package optimus.dsi.serialization.bson

import optimus.dsi.serialization.TypeMarkers

/**
 * Small marker objects stored in BSON to indicate a serialized supported "primitive"
 */
object BinaryMarkers extends TypeMarkers[Array[Byte]] {
  val Tuple: Array[Byte] = "~T".getBytes
  val FinalRef: Array[Byte] = "~E".getBytes
  val VersionedRef: Array[Byte] = "~V".getBytes
  val BusinessEventRef: Array[Byte] = "~B".getBytes
  val LocalDate: Array[Byte] = "~D".getBytes
  val LocalTime: Array[Byte] = "~t".getBytes
  val ModuleEntityToken: Array[Byte] = "~M".getBytes
  val OffsetTime: Array[Byte] = "~o".getBytes
  val ZonedDateTime: Array[Byte] = "~Z".getBytes
  val ZoneId: Array[Byte] = "~z".getBytes
  val Period: Array[Byte] = "~P".getBytes
  // NB: The ByteArray Marker is not used for MongoDoc serialization!
  val ByteArray: Array[Byte] = "~BA".getBytes()
}
