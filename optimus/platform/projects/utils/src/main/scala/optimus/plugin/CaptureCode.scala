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
package optimus.plugin

/*
  To be used in conjuction with TypedUtils#CaptureCode, e.g.
    @node def nodeFunc(i: Int) = i + 1
    @node def foo: (String, Int) = CaptureCode("optimus_autoasync",Some(1).map(nodeFunc))
  will return something like
    ("optimus.platform.this.async.seq(Some(1)).map(nodeFunc)", Some(i + 1))

 */
object CaptureCode {

  def apply[T](phase: String)(x: T): (String, T) = ("not captured", x)

}
