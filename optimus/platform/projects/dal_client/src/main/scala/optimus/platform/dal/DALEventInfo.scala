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
package optimus.platform.dal

import java.time.Instant

sealed abstract class DALEventInfo {
  def lockToken: Long
  def vid: Int
  def txTime: Instant
  def amend: DALEventInfo = ???
}

case object LocalDALEventInfo extends DALEventInfo {
  def lockToken = 0
  def vid = 1
  def txTime = null
}

// NB: txTime is ttf on the underlying stored event, NOT the loadContext.
final case class DSIEventInfo(vid: Int, lockToken: Long, txTime: Instant) extends DALEventInfo {
  override def amend = {
    DSIAmendEventInfo(vid + 1, lockToken)
  }
}

final case class DSIAmendEventInfo(vid: Int, lockToken: Long) extends DALEventInfo {
  override def txTime = null
}
