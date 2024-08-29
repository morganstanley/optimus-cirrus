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

import optimus.platform._
import optimus.platform.storable.PersistentEntity
import scala.util.{Success, Failure}
import optimus.platform.dsi.bitemporal.ReferenceQuery
import optimus.platform.storable.EntityReference

trait DALDebug {
  def resolver: ResolverImpl

  object debug {
    @node
    def getByRef(ref: EntityReference): PersistentEntity = {
      val qt = QueryTemporality.At(DALImpl.validTime, DALImpl.transactionTime)
      resolver.findPersistentEntitiesAt(ReferenceQuery(ref), qt) match {
        case Success(iter) =>
          iter.toList match {
            case pe :: Nil => pe
            case Nil       => null
            case o =>
              throw new GeneralDALException(
                s"DAL byReference query returned ${o.size} many results. " +
                  s"This should never happen and indicates corrupt data (overlapping bi-temporal rectangles). " +
                  s"The version references are ${o map { _.versionedRef } mkString (",")}")
          }
        case Failure(e) => throw e
      }
    }

    @node
    def getAllByRef(ref: EntityReference): Iterable[PersistentEntity] = {
      val qt = QueryTemporality.All
      resolver.findPersistentEntities(ReferenceQuery(ref), qt) match {
        case Success(pes) => pes
        case Failure(e)   => throw e
      }
    }
  }
}
