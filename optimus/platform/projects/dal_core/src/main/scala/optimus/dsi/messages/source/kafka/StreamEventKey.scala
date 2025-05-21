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
package optimus.dsi.messages.source.kafka
import optimus.dsi.messages.source.kafka.StreamEventKey.recordKeyProperties
import optimus.platform.dal.messages.MessagesTransactionPayload
import optimus.platform.dsi.bitemporal.WriteBusinessEvent
import optimus.platform.storable.Entity
import optimus.platform.storable.SerializedEntity
import optimus.utils.CollectionUtils._

// Note that sortedPropertyValues is Seq not Array here (even though it is an Array in SortedPropertyValues class)
// because AbstractTypeTransformerOps.serialize handles Seqs
private[optimus] final case class StreamEventKey private[optimus] (sortedPropertyValues: Seq[Any]) {
  private[messages] def matches(put: WriteBusinessEvent.Put, className: String): Boolean = {
    val ent = put.ent
    if (ent.className != className) false
    else {
      val entityKeyProperties = recordKeyProperties(ent)
      val isMatch = entityKeyProperties sameElements sortedPropertyValues
      isMatch
    }
  }

  def findEntityInTransaction(className: String, transaction: MessagesTransactionPayload): SerializedEntity = {
    val be = transaction.message.putEvent.bes.singleOrThrow(_ => "Expected single business event!")
    val pes = be.puts.filter(matches(_, className))
    val pe = pes.singleOrThrow(_ => "Expected single key to match")
    pe.ent
  }

  // to be used only where single key is expected
  def singlePropertyValue: Any = sortedPropertyValues.singleOrThrow(_ => StreamEventKey.singleKeyMsg)
}
private[optimus] object StreamEventKey {
  val singleKeyMsg: String = "Expected single @key -- unique and not indexed"

  // See optimus.platform.pickling.EmbeddablePicklers.Tag
  val Tag: String = "_tag"

  private[kafka] def recordKeyProperties(entity: SerializedEntity): Array[Any] = {
    // TODO (OPTIMUS-64332): could support multiple keys here (see StreamKafkaConsumerListener.notifyMessages)
    val entityKey = entity.keys.filter(_.isKey).singleOrThrow(_ => singleKeyMsg)
    entityKey.properties.sortedPropertyValues
  }

  private[kafka] def from(entity: SerializedEntity): StreamEventKey = StreamEventKey(recordKeyProperties(entity))
}
