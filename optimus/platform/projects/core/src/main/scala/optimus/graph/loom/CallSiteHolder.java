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
import static optimus.graph.loom.NameMangler.mangleName;
import static optimus.graph.loom.NameMangler.mkLoomName;
import static optimus.graph.loom.NameMangler.stripNewNodeSuffix;
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

  /**
   * TODO (OPTIMUS-66991): A few improvements to consider before using this in PROD (this is
   * currently behind a flag!):
   * <li>Consider threading implications
   * <li>Consider generating a handle once, store/cache it...
   * <li>Maybe hold off less items here (e.g, info, nti also can be passed in)
   * <li>NTI.callSite should be private
   * <li>Tweaked should probably go via specialized path as well
   * <li>OGTrace observer switch should flip the call sites
   */
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
    var name = mkLoomName(mangleName(entityCls), stripNewNodeSuffix(info.getName()));
    var factoryType = mutableCallsite.type();
    var methodType = info.getMethodType().changeReturnType(factoryType.returnType());
    try {
      return caller.findVirtual(entityCls, name, methodType);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  // we are not ready yet! (see comment on top of the file)
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
