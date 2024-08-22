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
package optimus.platform.relational.dynamic

import java.io.ByteArrayOutputStream

import optimus.platform._
import optimus.platform.relational.aggregation.UntypedAggregation
import optimus.platform.relational.formatter.CellFormatter
import optimus.platform.relational.formatter.OutputSettings

trait DynamicObjectQueryExtensions {

  implicit class DynamicObjectQuery(query: Query[DynamicObject]) {
    def sort(sortRules: List[(String, Boolean)]): Query[DynamicObject] = {
      val rule = DynamicObjSortRule(sortRules)
      query.sortBy({ x: DynamicObject =>
        DynamicObjSortOrderable(x, rule)
      })
    }

    def project(fieldsToProject: Seq[String]): Query[DynamicObject] = {
      query.shapeTo(obj => DynamicObjectHelper.createDynamicContainerObj(obj, fieldsToProject))
    }

    def aggregate(aggregateByFieldNames: Seq[String], aggregationRules: Seq[UntypedAggregation]): Query[DynamicObject] =
      query.aggregateByUntyped(aggregateByFieldNames.toSet, aggregationRules: _*)

    def renameColumns(renaming: Map[String, String]): Query[DynamicObject] = {
      query.shapeTo(obj => DynamicObjectHelper.createDynamicContainerObj(obj, renaming))
    }

    def mkString(cellFormatters: Map[String, CellFormatter[_]] = Map(), convertToUnix: Boolean = true): String = {
      val os: ByteArrayOutputStream = new ByteArrayOutputStream()
      implicit val settings = OutputSettings(cellFormatters)
      Query.displayTo(query, os)
      val s = os.toString
      if (convertToUnix) s.replace("\r\n", "\n") else s
    }

    def dropColumns(columnsToDrop: Set[String]): Query[DynamicObject] = {
      query.shapeTo(obj =>
        DynamicObjectHelper.createDynamicContainerObj(obj, (obj.getAll.keySet -- columnsToDrop).toSeq))
    }
  }
}
