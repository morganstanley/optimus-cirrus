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
package optimus.graph.loom;

import static java.lang.invoke.MethodHandles.filterReturnValue;
import static optimus.CoreUtils.stripSuffix;
import static optimus.graph.loom.LoomConfig.LOOM_SUFFIX;
import static optimus.graph.loom.LoomConfig.NEW_NODE_SUFFIX;
import static optimus.graph.loom.NodeMetaFactory.mhLookupAndGet;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;
import optimus.graph.DiagnosticSettings;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGTrace;

public class CallSiteHolder {
  final MutableCallSite mutableCallsite;
  private final MethodHandles.Lookup caller;
  private final Class<?> entityCls;
  private final NodeTaskInfo nti;
  private final MethodHandleInfo info;
  private final MethodHandle nodeMethod;

  CallSiteHolder(
      MethodHandles.Lookup caller,
      Class<?> entityCls,
      MethodType factoryType,
      NodeTaskInfo nti,
      MethodHandleInfo info,
      MethodHandle nodeMethod) {
    this.mutableCallsite = new MutableCallSite(factoryType);
    this.caller = caller;
    this.entityCls = entityCls;
    this.nti = nti;
    this.info = info;
    this.nodeMethod = nodeMethod;
    refreshTarget();
  }

  public void refreshTarget() {
    var handle = needsLookup() ? mhWithLookup() : mhLoom();
    mutableCallsite.setTarget(handle.asType(mutableCallsite.type()));
  }

  // e.g., foo$newNode.lookupAndGet
  private MethodHandle mhWithLookup() {
    return filterReturnValue(nodeMethod, mhLookupAndGet);
  }

  // e.g., foo$_
  private MethodHandle mhLoom() {
    var name = stripSuffix(info.getName(), NEW_NODE_SUFFIX) + LOOM_SUFFIX;
    var factoryType = mutableCallsite.type();
    var methodType = info.getMethodType().changeReturnType(factoryType.returnType());
    try {
      return caller.findVirtual(entityCls, name, methodType);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  // we are not ready yet...
  private static final boolean disableOpt =
      DiagnosticSettings.getBoolProperty("optimus.loom.disableMCS", true);

  private boolean needsLookup() {
    return disableOpt
        || nti.isDirectlyTweakable()
        || nti.getCacheable()
        || nti.shouldLookupPlugin()
        || OGTrace.observer.collectsHotspots();
  }
}
