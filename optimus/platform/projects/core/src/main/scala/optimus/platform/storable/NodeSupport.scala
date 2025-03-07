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
package optimus.platform.storable

import optimus.graph._
import optimus.graph.cache.CacheSelector

object NodeSupport {

  def lookupConstructorCache[T](t: T): T = {
    val pInfo = ConstructorNode.buildConstructorInfo(t.getClass)
    if (pInfo.getCacheable) {
      /* This loads the current context, but it might be that the constructor node is loaded as part of class
       * initialization or some other non-graph reason. In that case it will still go to the private cache, which isn't
       * really what we want, but fixing that would be very complicated. */
      // relying on OGSchedulerContext.current() here is not an RT violation because the constructor itself is RT
      val ec = OGSchedulerContext._TRACESUPPORT_unsafe_current()
      val privateCache = if (ec eq null) CacheSelector.DEFAULT else ec.scenarioStack().cacheSelector.withoutScope()
      val cNode = new ConstructorNode[T](t, pInfo)
      val n = privateCache.lookupAndInsert(pInfo, cNode, null)
      n.result // no isDoneWithResult check because these are always SI ACPNs
    } else
      t
  }
}
