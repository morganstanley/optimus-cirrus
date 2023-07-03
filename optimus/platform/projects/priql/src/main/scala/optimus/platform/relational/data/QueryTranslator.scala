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
package optimus.platform.relational.data

import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.mapping.QueryMapping
import optimus.platform.relational.tree.RelationElement

class QueryTranslator(val language: QueryLanguage, val mapping: QueryMapping) {
  val mapper = mapping.createMapper(this)
  val dialect = language.createDialect(this)
  val entityLookup = language.lookup

  def translate(element: RelationElement): RelationElement = {
    var e = mapper.translate(element)
    e = dialect.translate(e)
    e
  }
}
