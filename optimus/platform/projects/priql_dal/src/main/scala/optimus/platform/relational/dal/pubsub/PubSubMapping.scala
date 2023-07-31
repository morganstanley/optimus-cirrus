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
package optimus.platform.relational.dal.pubsub

import optimus.entity.EntityInfoRegistry
import optimus.platform.relational.dal.core._
import optimus.platform.relational.data.DataProvider
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.mapping._
import optimus.platform.relational.tree.TypeInfo

class PubSubMapping extends DALMapping {
  override def createMapper(translator: QueryTranslator): PubSubMapper = {
    new PubSubMapper(this, translator)
  }

  protected override def getEntityImpl(shape: TypeInfo[_]): MappingEntity = {
    val info = EntityInfoRegistry.getClassInfo(shape.clazz)
    PubSubMappingEntityFactory.create(info, shape, shape.runtimeClassName)
  }
}

class PubSubRegisteredIndexMapping extends PubSubMapping {
  override def isProviderSupported(dp: DataProvider): Boolean = DALRegisteredIndexMapping.isProviderSupported(dp)
}

class PubSubMapper(m: PubSubMapping, t: QueryTranslator) extends DALMapper(m, t) {
  override val binder: QueryBinder = PubSubBinder
}
