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
package optimus.platform.util
import java.util.concurrent.CompletableFuture

object ProcessUtils {
  def killProcessAndDescendants(p: Process): Unit = {
    val ph = p.toHandle
    val victims = Seq.newBuilder[CompletableFuture[_]]
    ph.descendants().forEach { descendant =>
      descendant.destroy()
      victims += descendant.onExit()
    }
    p.destroy()
    victims += p.onExit()

    // java.lang.Process is synchronous right? Nope! For some reason, process termination is async! It's the only method
    // for which that is the case! Isn't that an exciting source of "fun"?
    CompletableFuture.allOf(victims.result(): _*).join()
  }
}
