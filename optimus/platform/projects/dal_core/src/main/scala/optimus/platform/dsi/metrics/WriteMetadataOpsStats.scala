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
package optimus.platform.dsi.metrics

import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.immutable.ListMap
import optimus.scalacompat.collection._

/**
 * Data supposed to be passed to the DAL metrics related logic and stored along with other metrics. Can't be @embeddable
 * since there are no entityplugin and required picklers available in this module.
 */
final case class WriteMetadataOpsStats(
    total: Int,
    eventOps: WriteEventMetadataOpsStats,
    entityOps: WriteEntityMetadataOpsStats) {

  def asJson: JsValue =
    JsObject(
      fields = ListMap(
        "total" -> total.toString.toJson,
        "event" -> ListMap(
          "groupings" -> eventOps.groupings,
          "indexes" -> eventOps.indexes,
          "keys" -> eventOps.keys,
          "timeslices" -> eventOps.timeslices
        ).mapValuesNow(_.toString).toMap.toJson, // toMap for spray-json issue #65 [mapValues-2.13]
        "entity" -> ListMap(
          "eref_indexes" -> entityOps.erefIndexes,
          "groupings" -> entityOps.groupings,
          "indexes" -> entityOps.indexes,
          "keys" -> entityOps.keys,
          "linkages" -> entityOps.linkages,
          "timeslices" -> entityOps.timeslices,
          "unq_idx_groupings" -> entityOps.unqIdxGroupings,
          "unq_idx_timeslices" -> entityOps.unqIdxTimeslices
        ).mapValuesNow(_.toString).toMap.toJson
      ))
}

object WriteMetadataOpsStats {
  lazy val empty: WriteMetadataOpsStats = WriteMetadataOpsStats(
    total = 0,
    WriteEventMetadataOpsStats(0, 0, 0, 0),
    WriteEntityMetadataOpsStats(0, 0, 0, 0, 0, 0, 0, 0)
  )
}

final case class WriteEventMetadataOpsStats(
    groupings: Int,
    indexes: Int,
    keys: Int,
    timeslices: Int
)

final case class WriteEntityMetadataOpsStats(
    erefIndexes: Int,
    groupings: Int,
    indexes: Int,
    keys: Int,
    linkages: Int,
    timeslices: Int,
    unqIdxGroupings: Int,
    unqIdxTimeslices: Int
)
