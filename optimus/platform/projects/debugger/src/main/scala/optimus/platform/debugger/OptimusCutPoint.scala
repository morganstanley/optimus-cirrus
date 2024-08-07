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
package optimus.platform.debugger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.ScalaObjectMapper

/**
 * A class used to serialize data about methods which are used to restart optimus nodes in the IJ debugger.
 */
final case class OptimusCutPoint(className: String, methodName: String)

object OptimusCutPoint {
  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def write(elem: OptimusCutPoint): String =
    mapper.writeValueAsString(elem)

  def read(s: String): OptimusCutPoint =
    mapper.readValue(s, classOf[OptimusCutPoint])
}
