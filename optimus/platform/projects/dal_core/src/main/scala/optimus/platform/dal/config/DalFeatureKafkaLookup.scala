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
package optimus.platform.dal.config
import optimus.dsi.cli.ZkPaths

private[optimus] object KafkaFeature extends Enumeration {
  val Messages: KafkaFeature.Value = Value("/messages")
  val Uow: KafkaFeature.Value = Value("/uow")
  val KafkaSourceWrite: KafkaFeature.Value = Value("/kafka_source_write")
  val Streams: KafkaFeature.Value = Value("/streams")
}

private[optimus] final case class DalFeatureKafkaLookup(
    env: DalEnv,
    instance: Option[String],
    featurePath: KafkaFeature.Value
) {
  val connectionString: String = s"${ZkPaths.ZK_DAL_SERVICE_NODE}/${env.mode}$featurePath"
}

object DalFeatureKafkaLookup {
  def apply(env: DalEnv, featurePath: KafkaFeature.Value): DalFeatureKafkaLookup = {
    DalFeatureKafkaLookup(env, None, featurePath)
  }
}
