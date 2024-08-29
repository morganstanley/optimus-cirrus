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
package optimus.dal.silverking.client

import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective
import com.ms.silverking.cloud.dht.client.BaseNamespacePerspective
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective

object TraceableSkConverters {
  def serverSupportsTrace[K, V](original: BaseNamespacePerspective[K, V]): Boolean = {
    original.getNamespace.isServerTraceEnabled
  }

  /**
   * Covert a opened vanilla AsyncNP into traceable mode (sk client will reject "WithTrace call" if server doesn't
   * support tracing) <b>NOTE:</b> Vanilla SK API will still work (via call asVanilla())
   */
  def asTraceable[K, V](original: AsynchronousNamespacePerspective[K, V]): TraceableAsyncNamespacePerspective[K, V] = {
    new TraceableAsyncNamespacePerspective(original)
  }

  /**
   * Covert a opened vanilla SyncNP into traceable mode (sk client will reject "WithTrace call" if server doesn't
   * support tracing) <b>NOTE:</b> Vanilla SK API will still work (via call asVanilla())
   */
  def asTraceable[K, V](original: SynchronousNamespacePerspective[K, V]): TraceableSyncNamespacePerspective[K, V] = {
    new TraceableSyncNamespacePerspective(original)
  }

  /**
   * Try to covert AsynchronousNamespacePerspective to traceable mode (will return None if server doesn't support trace
   * or disabled trace)
   */
  def tryAsTraceable[K, V](
      original: AsynchronousNamespacePerspective[K, V]): Option[TraceableAsyncNamespacePerspective[K, V]] = {
    if (serverSupportsTrace(original)) {
      Some(new TraceableAsyncNamespacePerspective(original))
    } else {
      None
    }
  }

  /**
   * Try to covert SynchronousNamespacePerspective to traceable mode (will return None if server doesn't support trace
   * or disabled trace)
   */
  def tryAsTraceable[K, V](
      original: SynchronousNamespacePerspective[K, V]): Option[TraceableSyncNamespacePerspective[K, V]] = {
    if (serverSupportsTrace(original)) {
      Some(new TraceableSyncNamespacePerspective(original))
    } else {
      None
    }
  }
}
