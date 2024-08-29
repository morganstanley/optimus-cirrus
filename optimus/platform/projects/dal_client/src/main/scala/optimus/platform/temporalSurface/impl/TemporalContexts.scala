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
package optimus.platform.temporalSurface.impl

import java.io.ObjectStreamException
import java.time.Instant

import optimus.graph.Settings
import optimus.platform._
import optimus.platform._
import optimus.platform.internal.IgnoreSyncStacksPlugin
import optimus.platform.scenarioIndependent
import optimus.platform.temporalSurface._
import optimus.utils.MacroUtils.SourceLocation

private[optimus /*platform*/ ] object FlatTemporalSurface {
  @entity private object factory {
    // really these are just a cacheable functional call
    @scenarioIndependent @node def create(
        matcher: TemporalSurfaceMatcher,
        vt: Instant,
        tt: Instant,
        tag: Option[String],
        sourceLocation: SourceLocation): FlatTemporalSurface =
      new FlatTemporalSurface(matcher, vt, tt, tag, sourceLocation)

    IgnoreSyncStacksPlugin.installIfNeeded(create_info)
  }
  def apply(matcher: TemporalSurfaceMatcher, vt: Instant, tt: Instant, tag: Option[String])(implicit
      sourceLocation: SourceLocation): FlatTemporalSurface =
    factory.create(matcher, vt, tt, tag, sourceLocation)

  // until we resolve the temporal surface throughout the code, some legacy APIs
  def apply(vt: Instant, tt: Instant, tag: Option[String]): FlatTemporalSurface =
    FlatTemporalSurface(DataFreeTemporalSurfaceMatchers.all, vt, tt, tag)

}
@entity private[optimus /*platform*/ ] object FlatTemporalContext {
  @entity private object factory {
    // really this is just a cacheable functional call
    @scenarioIndependent @node def create(
        matcher: TemporalSurfaceMatcher,
        vt: Instant,
        tt: Instant,
        tag: Option[String],
        sourceLocation: SourceLocation): FixedTemporalContextImpl with FlatTemporalContext = {
      val result = new FlatTemporalContext(matcher, vt, tt, tag, sourceLocation)
      if (Settings.traceCreateTemporalContext) {
        TemporalContextTrace.traceCreated(result)
      }
      result
    }
    IgnoreSyncStacksPlugin.installIfNeeded(create_info)
    if (Settings.traceCreateTemporalContext) {
      // trace every call
      create_info.setCacheable(false)
    }
  }
  @scenarioIndependent @node def apply(matcher: TemporalSurfaceMatcher, vt: Instant, tt: Instant, tag: Option[String])(
      implicit sourceLocation: SourceLocation): FixedTemporalContextImpl with FlatTemporalContext =
    factory.create(matcher, vt, tt, tag, sourceLocation)

  private def buildTC(
      matcher: TemporalSurfaceMatcher,
      vt: Instant,
      tt: Instant,
      tag: Option[String],
      sourceLoc: SourceLocation): FixedTemporalContextImpl with FlatTemporalContext =
    factory.create(matcher, vt, tt, tag, sourceLoc)

  apply_info.setCacheable(false)
  // until we resolve the temporal surface throughout the code, some legacy APIs

  def apply(vt: Instant, tt: Instant, tag: Option[String])(implicit
      sourceLocation: SourceLocation): FixedTemporalContextImpl with FlatTemporalContext =
    AdvancedUtils.suppressSyncStackDetection(
      FlatTemporalContext.buildTC(DataFreeTemporalSurfaceMatchers.all, vt, tt, tag, sourceLocation))
}

final private[optimus /*platform*/ ] class FlatTemporalSurface(
    val matcher: TemporalSurfaceMatcher,
    private[optimus /*platform*/ ] val vt: Instant,
    private[optimus /*platform*/ ] val tt: Instant,
    override private[optimus] val tag: Option[String],
    override private[optimus] val sourceLocation: SourceLocation)
    extends FixedLeafTemporalSurfaceImpl {

  /**
   * for serialisation, use the cache if it is available
   */
  @throws(classOf[ObjectStreamException])
  final def readResolve(): AnyRef =
    if (EvaluationContext.isInitialised) FlatTemporalSurface(matcher, vt, tt, tag)(sourceLocation) else this

  override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[FlatTemporalSurface]

}

final private[optimus /*platform*/ ] class FlatTemporalContext(
    val matcher: TemporalSurfaceMatcher,
    private[optimus /*platform*/ ] val vt: Instant,
    private[optimus /*platform*/ ] val tt: Instant,
    override private[optimus] val tag: Option[String],
    override private[optimus] val sourceLocation: SourceLocation)
    extends FixedLeafTemporalSurfaceImpl
    with FixedTemporalContextImpl
    with FixedLeafTemporalContext {

  override def unsafeValidTime: Instant = vt
  override def unsafeTxTime: Instant = tt

  /**
   * for serialisation, use the cache if it is available
   */
  @throws(classOf[ObjectStreamException])
  final def readResolve(): AnyRef =
    if (EvaluationContext.isInitialised) FlatTemporalContext(matcher, vt, tt, tag)(sourceLocation) else this

  override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[FlatTemporalContext]

}

/**
 * Note - this is accessed via ServiceLoader discovery
 */
class FlatTemporalSurfaceFactoryImpl extends FlatTemporalSurfaceFactory {
  private[optimus] def createTemporalContext(vt: Instant, tt: Instant, tag: Option[String]): FixedLeafTemporalContext =
    FlatTemporalContext(vt, tt, tag)
}
