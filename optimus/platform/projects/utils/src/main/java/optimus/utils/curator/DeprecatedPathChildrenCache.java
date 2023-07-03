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
package optimus.utils.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;

public class DeprecatedPathChildrenCache extends PathChildrenCache {

  public interface StartMode {
    PathChildrenCache.StartMode BUILD_INITIAL_CACHE =
        PathChildrenCache.StartMode.BUILD_INITIAL_CACHE;
  }

  public DeprecatedPathChildrenCache(CuratorFramework client, String path, boolean cacheData) {
    super(client, path, cacheData);
  }
}
