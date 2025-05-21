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
package optimus.entity

import optimus.core.CoreHelpers

sealed trait LinkageType {
  val propertyName: String
  val typeName: String
}

final case class EntityLinkageProperty private (propertyName: String, typeName: String) extends LinkageType

// EntityLinkageProperty instances have low cardinality so we strong-intern them
object EntityLinkageProperty {
  def apply(propertyName: String, typeName: String): EntityLinkageProperty = {
    CoreHelpers.strongIntern(new EntityLinkageProperty(propertyName, typeName)).asInstanceOf[EntityLinkageProperty]
  }
}
