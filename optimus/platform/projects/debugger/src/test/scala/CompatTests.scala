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
import optimus.platform.debugger.StackElemData
import optimus.platform.debugger.StackElemType
import org.junit.Test

class CompatTests {

  @Test def backAndForthTest(): Unit = {
    val x = StackElemData("foo", "bar", "(node) foo", 10, StackElemType.jvm)
    val data = StackElemData.write(x)
    val y = StackElemData.read(data)
    assert(x == y)
  }

  @Test def testBackwardCompat(): Unit = {
    // Test that we can handle the addition of a new StackElemType
    val x = StackElemData("foo", "bar", "(node) foo", 10, StackElemType.jvm)
    val data = StackElemData.write(x).replaceAll("jvm", "haskell")
    val y = StackElemData.read(data)
    assert(y == StackElemData("foo", "bar", "(node) foo", 10, StackElemType.unknown))
  }

}
