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
package optimus.platform.inputs.registry

import optimus.platform.inputs.ProcessInputs
import optimus.platform.inputs.DefaultSerializableProcessSINodeInput

import java.time.Duration

object ProcessDistInputs {
  val Dmc2RequestRegistryTimeout: DefaultSerializableProcessSINodeInput[Duration] =
    ProcessInputs.newSerializableNoSideEffects(
      "Dmc2RequestRegistryTimeout",
      "How long to wait before timing out on in flight DMC request",
      Duration.ofMinutes(1),
      Source.fromDurJavaProperty("optimus.dist.dmc2RequestRegistryTimeout")
    )
}
