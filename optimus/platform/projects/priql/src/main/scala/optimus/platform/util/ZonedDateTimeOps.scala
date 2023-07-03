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
package optimus.platform.util

import java.time.ZonedDateTime
import optimus.platform.relational.tree.RuntimeMethodDescriptor
import optimus.platform.relational.tree._

// This is used as an intermediate representation when a following priql expression is added:
//    from(Entity).filter(e => e.zdt.richEqualInstant(otherZdt))
//
// An implicit class RichZonedDateTime defined in optimus.platform.relational package comes into play
// and at runtime when priql is converted into a tree, it will be converted into
// "ZonedDateTimeOps.equalInstant(zdt, otherZdt)" for correct type representation in tree.
object ZonedDateTimeOps {
  def equalInstant(zdt: ZonedDateTime, otherZdt: ZonedDateTime): Boolean = zdt.toInstant == otherZdt.toInstant
  def runtimeMethodDescriptor: RuntimeMethodDescriptor =
    new RuntimeMethodDescriptor(typeInfo[ZonedDateTimeOps.type], "equalInstant", TypeInfo.BOOLEAN)
}
