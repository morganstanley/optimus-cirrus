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
package optimus.platform.internal

import java.time.Instant
import optimus.core.NodeAPI
import optimus.graph.RTGraphException
import optimus.graph.GraphException
import optimus.graph.InstancePropertyTarget
import optimus.graph.PropertyNode
import optimus.graph.Settings
import optimus.graph.diagnostics.InfoDumper
import optimus.platform._
import optimus.platform.dal.DALImpl._
import optimus.platform.dal.Marker
import optimus.platform.temporalSurface.impl.TemporalContextTrace

@entity
object TemporalSource {
  private[optimus] val validTimeMarker = Marker[Instant](this, MarkerResolvers.validTimeMarker.asMarkerResolver)
  private[optimus] val transactionTimeMarker =
    Marker[Instant](this, MarkerResolvers.transactionTimeMarker.asMarkerResolver)
  private[optimus] val loadContextMarker = Marker(this, MarkerResolvers.loadContextMarker.asMarkerResolver)

  private[optimus] val validTimeStoreMarker =
    Marker(this, MarkerResolvers.validTimeStoreMarker.asMarkerResolver).asInstanceOf[PropertyNode[Instant]]

  def translateMarkers(markers: java.util.IdentityHashMap[Marker[_], Tweak]): Seq[Tweak] = {
    val vt = markers.get(validTimeMarker)
    val tt = markers.get(transactionTimeMarker)
    val lc = markers.get(loadContextMarker)
    val lcKey = NodeAPI.nodeKeyOf(this.loadContext)

    val vts = markers.get(validTimeStoreMarker)

    @entersGraph def cval[A](twk: Tweak): A = twk.computableValueNode[A](EvaluationContext.scenarioStack).await

    val lcTweak = Option {
      if (Settings.warnOnVtTtTweaked && ((vt ne null) || (tt ne null))) {
        TemporalContextTrace.traceTweakedVtTt(vt, tt)
      }

      if (lc ne null) {
        if ((tt ne null) || (vt ne null))
          throw new GraphException(
            "Cannot tweak loadContext with neither valid or transaction times in the same scenario")
        if (Settings.warnOnVtTtTweaked) TemporalContextTrace.traceTweakedLoadContext(lc)
        lc.retarget(new InstancePropertyTarget(lcKey))
      } else if ((vt ne null) && (tt ne null)) {
        SimpleValueTweak(lcKey)(TemporalContext(cval[Instant](vt), cval[Instant](tt)))
      } else if ((vt ne null) || (tt ne null)) {
        try {
          val tc = loadContext
          if (vt ne null)
            SimpleValueTweak(lcKey)(TemporalContext(cval[Instant](vt), tc.unsafeTxTime))
          else
            SimpleValueTweak(lcKey)(TemporalContext(tc.unsafeValidTime, cval[Instant](tt)))

        } catch {
          case _: UninitializedInitialTimeException => null
        }
      } else null
    }
    val vtsTweak = Option(
      if (vts ne null) SimpleValueTweak(NodeAPI.nodeKeyOf(validTimeStore))(cval[Instant](vts))
      else if (vt ne null) SimpleValueTweak(NodeAPI.nodeKeyOf(validTimeStore))(cval[Instant](vt))
      else null)

    Nil ++ lcTweak ++ vtsTweak
  }

  @node private[optimus] def validTimeAsNode = {
    if (Settings.warnOnVtTtAccessed) TemporalContextTrace.traceReadVt()

    loadContext.unsafeValidTime
  }
  @node private[optimus] def transactionTimeAsNode = {
    if (Settings.warnOnVtTtAccessed) TemporalContextTrace.traceReadTt()

    loadContext.unsafeTxTime
  }
  if (Settings.warnOnVtTtAccessed) {
    // if we want to log access then we don't want the cache to remember the value.
    // This is not a perfect solution, as a calling node may still remember it
    // but better than nothing
    validTimeAsNode_info.setCacheable(false)
    transactionTimeAsNode_info.setCacheable(false)
  }

  @node(tweak = true) private[optimus] def loadContext: TemporalContext = TemporalContext(initialTime, initialTime)

  @node(tweak = true) private[optimus] def validTimeStore: Instant = initialTime

  // Must always be tweaked.  RuntimeEnvironment creation installs a tweak in its top scenario.
  @node(tweak = true) def initialTime: Instant = uninitializedInitialTime()

  def uninitializedInitialTime(): Instant = {
    val msg = s"nodestack=${GraphException.pathAsString()}; scenarioStack=${GraphException.scenarioStackString()}"
    val e = new UninitializedInitialTimeException(msg)
    if (Settings.killOnUninitializedTime)
      InfoDumper.graphPanic("panic", s"Uninitialized initialTime $msg", 121, e, null)
    throw e
  }

  @node(tweak = true) def temporalContextFactory: TemporalContextFactory = LoadContextTemporalContextFactory

  initialTime_info.internalSetTweakMaskForDAL()
  IgnoreSyncStacksPlugin.installIfNeeded(validTimeStore_info, loadContext_info)
}

class UninitializedInitialTimeException(diags: String) extends RTGraphException("Uninitialized initialTime " + diags)
