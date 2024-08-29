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
package optimus.dsi.metrics

import optimus.platform.dsi.bitemporal.BlobMetrics

object DataStorageUtils {

  // combine metrics for multiple stores for read and write ops
  def getMetricsFromMultipleStores(stores: Set[BlobMetrics]): BlobMetrics = {
    val filtered = stores.collect { case BlobMetrics(size, p: PersistentStorage) => BlobMetrics(size, p) }

    def getCombinedResult(s: Set[BlobMetrics]): BlobMetrics = {
      val stores = s.map { _.store }
      val ds = stores.size match {
        case 1 => stores.head
        case _ => throw new IllegalArgumentException(s"combination of ${stores} is not allowed")

      }
      BlobMetrics(s.map { _.size }.sum, ds)
    }

    filtered.size match {
      case 0 => BlobMetrics.empty
      case 1 => filtered.head
      case _ => getCombinedResult(filtered)
    }
  }
}

sealed trait DataStorage
sealed trait PersistentStorage extends DataStorage
sealed trait TransientStorage extends DataStorage

case object MongoDb extends PersistentStorage
case object PostgresDb extends PersistentStorage
case object ArchiveBlobStore extends PersistentStorage
case object InMemory extends TransientStorage
case object Cache extends TransientStorage
