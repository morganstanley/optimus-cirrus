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
package optimus.platform

import java.time.Instant

import optimus.dsi.partitioning.Partition
import optimus.platform.dal.DALImpl
import optimus.platform.temporalSurface.impl.FlatTemporalContext
import optimus.platform.temporalSurface.impl.TemporalContextImpl

sealed trait TemporalContextFactory extends Serializable {
  @node def generator: TemporalContextGenerator
}
case object LoadContextTemporalContextFactory extends TemporalContextFactory {
  @node override def generator = TemporalContextGenerator.from(DALImpl.loadContext)
}
case object FlatTemporalContextFactory extends TemporalContextFactory {
  @node override def generator = TemporalContextGenerator.FlatTemporalContextGenerator
}
//this is a hook to allow optimus objects an trait to provide custom implementation without exposing this to users of the platform
private[optimus] trait ExtensibleTemporalContextFactory extends TemporalContextFactory

trait TemporalContextGenerator extends Serializable {
  def temporalContext(vt: Instant, tt: Instant, tickTts: Map[Partition, Instant] = Map.empty): TemporalContextImpl
  def isTickable: Boolean
}
object TemporalContextGenerator {
  def from(context: TemporalContext): TemporalContextGenerator = context match {
    case context: TemporalContextImpl => Generator(context)
  }

  object FlatTemporalContextGenerator extends TemporalContextGenerator {
    val tag = Some("$$FlatTemporalContextGenerator")
    def temporalContext(vt: Instant, tt: Instant, tickTts: Map[Partition, Instant]) = FlatTemporalContext(vt, tt, tag)
    def isTickable = false
  }

  private final case class Generator(context: TemporalContextImpl) extends TemporalContextGenerator {
    override def temporalContext(vt: Instant, tt: Instant, tickTts: Map[Partition, Instant]): TemporalContextImpl =
      context
    override def isTickable: Boolean = context.canTick
  }
}
