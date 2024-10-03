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
package optimus.platform.storable

import optimus.platform.dsi.bitemporal.PutApplicationEvent

final case class SerializedUpsertableTransaction(
    putEvent: PutApplicationEvent
) extends UpsertableTransactionInfo {

  private def expectationMessage(
      msg: String
  ): String = s"DelayedTransaction used for PublishTransaction $msg"

  /**
   * Transaction must comply to specific constraints so it can be used via DAL.publishTransaction.
   *   - contains single newEvent {} block
   *   - if BusinessEvent-only message then it must be heap event
   *   - only DAL.upsert commands supported
   *     - no DAL.put command
   *     - no DAL.invalidate command
   *     - no DAL.revert command
   *   - can't contain Entity with @indexed(unique = true) property
   */
  def validate(): Unit = {
    // to keep it simple, only single newEvent {} block allowed
    require(
      putEvent.bes.size == 1,
      expectationMessage("should contain single newEvent block.")
    )

    val businessEvent = putEvent.bes.head // we expect single one, see above require

    require(
      writeBusinessEvent.state == EventStateFlag.NEW_EVENT || topLevelSerializedEntityClassNames.nonEmpty,
      expectationMessage("BusinessEvent must be heap instance OR transaction must contain DAL.upsert(s)")
    )

    // Keys for BusinessEvent should not contain @indexed (unique = true) properties
    businessEvent.evt.keys.foreach { key =>
      require(
        !(key.indexed && key.unique),
        expectationMessage(s"should not contain BusinessEvent / Unique-Indexed Key: $key Event: $businessEvent")
      )
    }

    // only DAL.upsert commands supported in delayedTransaction {} used for Transaction Messaging
    // lockToken is empty only for DAL.upsert action, for DAL.put it's provided
    require(
      putEvent.bes.forall(_.puts.forall(_.lockToken.isEmpty)),
      expectationMessage("should contain only DAL.upsert commands - DAL.put prohibited.")
    )

    require(
      putEvent.bes.forall(_.invalidates.isEmpty),
      expectationMessage("should contain only DAL.upsert commands - DAL.invalidate prohibited.")
    )

    require(
      putEvent.bes.forall(_.reverts.isEmpty),
      expectationMessage("should contain only DAL.upsert commands - DAL.revert prohibited.")
    )

    // Keys for SerializedEntities should not contain @indexed (unique = true) properties
    businessEvent.puts.foreach { p =>
      p.ent.keys.foreach { key =>
        require(
          !(key.indexed && key.unique),
          expectationMessage(
            s"should not contain Entity / Unique-Indexed Key: $key for Put: $p in Event: $businessEvent")
        )
      }
    }
  }
}
