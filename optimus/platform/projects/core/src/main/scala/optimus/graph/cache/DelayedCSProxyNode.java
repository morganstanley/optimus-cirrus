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
package optimus.graph.cache;

import optimus.graph.PropertyNode;
/**
 * DelayedCSProxyNode is created whenever a running node is found in cache with the same cacheID but
 * different CancellationScope to the key. There is therefore a high probability that re-use will be
 * possible (i.e. as long as the candidate hit isn't cancelled and doesn't fail with a non-RT
 * exception). If re-use turns out to not be possible then this proxy runs the underlying key
 * directly.
 */
public class DelayedCSProxyNode<T> extends DelayedProxyNode<T> {
  public DelayedCSProxyNode(PropertyNode<T> key, PropertyNode<T> hit) {
    super(key, hit);
  }
}
