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

import optimus.core.InternerHashEquals
import optimus.core.WeakInterner
import optimus.graph.Settings
import optimus.platform._
import optimus.platform.temporalSurface._
import optimus.utils.MacroUtils.SourceLocation

import java.io.ObjectStreamException
import java.time.Instant
import java.util.Objects

private[optimus /*platform*/ ] object FlatTemporalSurface {
  private val interner = new WeakInterner()

  def apply(matcher: TemporalSurfaceMatcher, vt: Instant, tt: Instant, tag: Option[String])(implicit
      sourceLocation: SourceLocation): FlatTemporalSurface =
    interner.intern(new FlatTemporalSurface(matcher, vt, tt, tag, sourceLocation)).asInstanceOf[FlatTemporalSurface]

  // until we resolve the temporal surface throughout the code, some legacy APIs
  def apply(vt: Instant, tt: Instant, tag: Option[String]): FlatTemporalSurface =
    FlatTemporalSurface(DataFreeTemporalSurfaceMatchers.all, vt, tt, tag)

}
@entity private[optimus /*platform*/ ] object FlatTemporalContext {
  private val interner = new WeakInterner()

  def apply(matcher: TemporalSurfaceMatcher, vt: Instant, tt: Instant, tag: Option[String])(implicit
      sourceLocation: SourceLocation): FixedTemporalContextImpl with FlatTemporalContext = {
    val result =
      interner.intern(new FlatTemporalContext(matcher, vt, tt, tag, sourceLocation)).asInstanceOf[FlatTemporalContext]
    if (Settings.traceCreateTemporalContext) {
      TemporalContextTrace.traceCreated(result)
    }
    result
  }

  def apply(vt: Instant, tt: Instant, tag: Option[String])(implicit
      sourceLocation: SourceLocation): FixedTemporalContextImpl with FlatTemporalContext =
    apply(DataFreeTemporalSurfaceMatchers.all, vt, tt, tag)
}

final private[optimus /*platform*/ ] class FlatTemporalSurface(
    val matcher: TemporalSurfaceMatcher,
    private[optimus /*platform*/ ] val vt: Instant,
    private[optimus /*platform*/ ] val tt: Instant,
    override private[optimus] val tag: Option[String],
    override private[optimus] val sourceLocation: SourceLocation)
    extends FixedLeafTemporalSurfaceImpl
    with InternerHashEquals {

  /**
   * for serialisation, use the cache if it is available
   */
  @throws(classOf[ObjectStreamException])
  final def readResolve(): AnyRef =
    if (EvaluationContext.isInitialised) FlatTemporalSurface(matcher, vt, tt, tag)(sourceLocation) else this

  override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[FlatTemporalSurface]

  // the regular hashcode / equals ignore the tag and location, but for interning we must include them
  override def internerHashCode(): Int = Objects.hash(matcher, vt, tt, tag, sourceLocation)
  override def internerEquals(other: Any): Boolean = other match {
    case that: FlatTemporalSurface =>
      Objects.equals(this.matcher, that.matcher) &&
      Objects.equals(this.vt, that.vt) &&
      Objects.equals(this.tt, that.tt) &&
      Objects.equals(this.tag, that.tag) &&
      Objects.equals(this.sourceLocation, that.sourceLocation)
    case _ => false
  }
}

final private[optimus /*platform*/ ] class FlatTemporalContext(
    val matcher: TemporalSurfaceMatcher,
    private[optimus /*platform*/ ] val vt: Instant,
    private[optimus /*platform*/ ] val tt: Instant,
    override private[optimus] val tag: Option[String],
    override private[optimus] val sourceLocation: SourceLocation)
    extends FixedLeafTemporalSurfaceImpl
    with FixedTemporalContextImpl
    with FixedLeafTemporalContext
    with InternerHashEquals {

  override def unsafeValidTime: Instant = vt
  override def unsafeTxTime: Instant = tt

  /**
   * for serialisation, use the cache if it is available
   */
  @throws(classOf[ObjectStreamException])
  final def readResolve(): AnyRef =
    if (EvaluationContext.isInitialised) FlatTemporalContext(matcher, vt, tt, tag)(sourceLocation) else this

  override protected def canEqual(ots: TemporalSurface): Boolean = ots.isInstanceOf[FlatTemporalContext]

  // the regular hashcode / equals ignore the tag and location, but for interning we must include them
  override def internerHashCode(): Int = Objects.hash(matcher, vt, tt, tag, sourceLocation)
  override def internerEquals(other: Any): Boolean = other match {
    case that: FlatTemporalContext =>
      Objects.equals(this.matcher, that.matcher) &&
      Objects.equals(this.vt, that.vt) &&
      Objects.equals(this.tt, that.tt) &&
      Objects.equals(this.tag, that.tag) &&
      Objects.equals(this.sourceLocation, that.sourceLocation)
    case _ => false
  }
}

/**
 * Note - this is accessed via ServiceLoader discovery
 */
class FlatTemporalSurfaceFactoryImpl extends FlatTemporalSurfaceFactory {
  private[optimus] def createTemporalContext(vt: Instant, tt: Instant, tag: Option[String]): FixedLeafTemporalContext =
    FlatTemporalContext(vt, tt, tag)
}
