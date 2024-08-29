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
package optimus.platform.dal.messages

// for source & sink topics, we retrieve base classes and build hierarchicalEntities
// for internal topics, they cannot be mapped to any entities, so we treat them differently
final case class StreamsACLs(
    entitlement: StreamsEntitlement,
    hierarchicalEntities: Seq[Seq[String]],
    internalTopics: Seq[String]
)

sealed trait StreamsEntitlement
object StreamsEntitlement {
  case object Produce extends StreamsEntitlement
  case object Consume extends StreamsEntitlement
}
