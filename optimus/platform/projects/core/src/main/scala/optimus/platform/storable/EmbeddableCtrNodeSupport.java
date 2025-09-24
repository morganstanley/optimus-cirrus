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
package optimus.platform.storable;

import java.util.Arrays;
import optimus.CoreUtils;
import optimus.config.NodeCacheConfigs$;
import optimus.core.CoreHelpers;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGTrace;

public class EmbeddableCtrNodeSupport {
  public static final String suffix = "-constructor";
  private static NodeTaskInfo[] ctorInfo = new NodeTaskInfo[16];
  private static int ctorCount = 0;

  private static void applyConfig(NodeTaskInfo info) {
    var key = CoreUtils.stripSuffix(info.name(), suffix);
    var constructorConfig = NodeCacheConfigs$.MODULE$.constructorConfigs().get(key);
    if (constructorConfig.isDefined()) info.setExternalConfig(constructorConfig.get());
  }

  /** Called when a new configuration is applied to the entire process */
  public static void reapplyConfig() {
    for (int i = 1; i < ctorInfo.length; i++) {
      var info = ctorInfo[i];
      if (info == null) break; // we have reached the end of the usable array
      applyConfig(info);
    }
  }

  /** Called once per class to register the id. Class is used only for the name */
  public static int registerID(Class<?> key) {
    synchronized (EmbeddableCtrNodeSupport.class) {
      int id = ++ctorCount;
      if (id >= ctorInfo.length) ctorInfo = Arrays.copyOf(ctorInfo, ctorInfo.length * 2);

      var keyName = CoreUtils.stripSuffix(key.getName(), "$");
      var info = new NodeTaskInfo(keyName + suffix, NodeTaskInfo.SCENARIOINDEPENDENT);
      applyConfig(info);
      ctorInfo[id] = info;
      return id;
    }
  }

  public static void setCacheable(int id, boolean cacheable) {
    ctorInfo[id].setCacheable(cacheable);
  }

  /** Generally just interns, but allows configuration to turn off the interning */
  public static <T> T lookupConstructorCache(T t, int id) {
    var pInfo = ctorInfo[id]; // ID has been allocated before and has to be present
    if (pInfo.getCacheable()) {
      var startTime = OGTrace.observer.lookupStart();
      var r = CoreHelpers.nonStrictIntern(t);
      OGTrace.observer.lookupAdjustCacheStats(pInfo, r != t, startTime);
      //noinspection unchecked
      return (T) r;
    } else return t;
  }
}
