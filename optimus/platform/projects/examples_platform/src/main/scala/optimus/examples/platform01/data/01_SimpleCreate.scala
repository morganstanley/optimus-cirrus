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
package optimus.examples.platform01.data

import optimus.examples.platform.entities.SimpleEntity
import optimus.examples.platform.entities.SimpleEvent
import org.kohsuke.args4j._
import optimus.platform._

class SupplyEntityNameArgs extends OptimusAppCmdLine {
  @Option(name = "-v", aliases = Array("--name"), usage = "Entity name")
  var name = System.getProperty("user.name") + "'s entity"
}

object SimpleCreate extends LegacyOptimusApp[SupplyEntityNameArgs] {
  val simpleEntity = SimpleEntity(cmdLine.name)

  newEvent(SimpleEvent.uniqueInstance("Create")) {
    DAL.put(simpleEntity)
  }

  println("Success the persist block exited and the entity was written: " + cmdLine.name)
}
