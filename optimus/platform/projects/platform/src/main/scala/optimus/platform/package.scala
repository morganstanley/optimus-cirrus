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
package optimus

import optimus.collection.OptimusDoubleSeq
import optimus.collection.OptimusSeq
import optimus.core.CoreAPI
import optimus.core.SourceAPI
import optimus.graph._
import optimus.platform.annotations.internal._fullTextSearch
import optimus.platform.annotations.internal._projected
import optimus.platform.annotations.internal._reified
import optimus.platform.dal.DalAPI
import optimus.platform.internal.CopyMethodMacros
import optimus.platform.internal.{CopyMethodMacros => _}
import optimus.platform.pickling.DefaultPicklers
import optimus.platform.pickling.DefaultUnpicklers
import optimus.platform.pickling.EntityCompanionView
import optimus.platform.relational.dal._
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityCompanionBase
import optimus.platform.util.NodeTryUtils
import optimus.tools.scalacplugins.entity.reporter.OptimusMessageRegistry
import optimus.utils.CollectionUtils
import optimus.utils.OptimusStringUtils
import optimus.utils.datetime.OrderingImplicits

import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import scala.annotation.compileTimeOnly
import scala.annotation.meta.field
import scala.annotation.meta.getter
import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

package object platform
    extends AsyncImplicits
    with Apars
    with CoreAPI
    with DalAPI
    with DefaultPicklers
    with DefaultUnpicklers
    with DALQueryExtensions
    with StorableConverterImplicits
    with SourceAPI
    with OrderingImplicits
    with CollectionUtils
    with OptimusStringUtils
    with NodeTryUtils {

  // TODO (OPTIMUS-29499): remove this compat stub
  type AdvancedOptimusApp[Args <: OptimusAppCmdLine] = OptimusApp[Args]

  type NodeFunction[-T, +R] = NodeFunction1[T, R]
  type projected = _projected @getter
  type fullTextSearch = _fullTextSearch @getter
  type intern = _intern @field
  type reified = _reified

  def push(scenario: Scenario): Unit = EvaluationContext.push(scenario)
  def pop(): Scenario = EvaluationContext.pop()

  // This gives the user a nice compiler message when trying to tweak a non-tweakable PropertyInfo
  @compileTimeOnly(OptimusMessageRegistry.Illegal_Property_Level_Tweak)
  implicit def propertyInfo2TweakTarget[E, WhenL <: AnyRef, SetL <: AnyRef, R](
      property: DefPropertyInfo[E, WhenL, SetL, R]): PropertyTarget[E, WhenL, SetL, R] = ???

  implicit def zonedDT2Instance(zdt: ZonedDateTime): Instant = zdt.toInstant
  implicit def offsetDT2Instance(odt: OffsetDateTime): Instant = odt.toInstant

  // transactionTimeNow, validTimeNow, and storeContextNow are the only ones allowed to use At.now as tweak body!
  @nowarn("msg=16000") @async def transactionTimeNow: Tweak = Tweak.byValue(transactionTime := At.now)
  @nowarn("msg=16000") @async def validTimeNow: Tweak = Tweak.byValue(validTime := At.now)
  @nowarn("msg=16000") @async def storeContextNow: Tweak = Tweak.byValue(storeContext := At.now)

  @async def validTimeAndTransactionTimeNow: Seq[Tweak] = {
    val t = At.now
    Tweaks.byValue(validTime := t, transactionTime := t)
  }

  @async def loadContextNow: Tweak = Tweak.byValue(loadContext := At.nowContext)

  @entersGraph def advanceTimeToNow: Unit = {
    val cn = EvaluationContext.currentNode
    val scenarioStack = cn.scenarioStack
    val tweaks = validTimeAndTransactionTimeNow
    val nss = scenarioStack.createChild(Scenario(tweaks), this)
    cn.replace(nss)
  }

  @entersGraph def advanceTimeToNowContext(): Unit = {
    val cn = EvaluationContext.currentNode
    val scenarioStack = cn.scenarioStack
    val tweaks = loadContextNow
    val nss = scenarioStack.createChild(Scenario(tweaks), this)
    cn.replace(nss)
  }

  @async def loadContextLsqt: Tweak = Tweak.byValue(loadContext := At.lsqtContext)
  @async def loadContextLsqtForVt(vt: Instant): Tweak = Tweak.byValue(loadContext := At.lsqtContextForVt(vt))

  implicit def entityCompanionView[T <: Entity](comp: EntityCompanionBase[T]): EntityCompanionView[T] =
    new EntityCompanionView(comp)

  // noinspection ScalaUnusedSymbol (used in macros but IDE doesn't recognize that)
  final class CopyUnique[E <: Entity] private[`package`] extends Dynamic {
    def applyDynamic(name: String)(args: Any*): E = macro CopyMethodMacros.copyUnique0Impl[E]
    def applyDynamicNamed(name: String)(args: (Any, Any)*): E = macro CopyMethodMacros.copyUniqueImpl[E]
  }

  implicit class EntityCopyOps[E <: Entity](e: E) {
    @compileTimeOnly("missing arglist for copyUnique")
    def copyUnique = new CopyUnique[E]
  }

  final class EventCopyUnique[E <: BusinessEvent] private[`package`] extends Dynamic {
    def applyDynamic(name: String)(args: Any*): E = macro CopyMethodMacros.eventCopyUnique0Impl[E]
    def applyDynamicNamed(name: String)(args: (Any, Any)*): E = macro CopyMethodMacros.eventCopyUniqueImpl[E]
  }

  implicit class EventCopyOps[E <: BusinessEvent](e: E) {
    @compileTimeOnly("missing arglist for copyUnique")
    def copyUnique = new EventCopyUnique[E]
  }

  // noinspection ScalaUnusedSymbol (used in macros but IDE doesn't recognize that)
  def asNodePF[T1, R](pf: PartialFunction[T1, R]): OptimusPartialFunction[T1, R] =
    macro optimus.platform.internal.PartialFunctionMacro.pfToOpf[T1, R]

  implicit def dalApi: this.type = this

  implicit class ExceptionWithNodeTrace(val t: Throwable) extends AnyVal {
    def getNodeTrace: Option[String] = Option(NodeTask.getNodeTrace(t))
    def getSimpleNodeTrace: Option[String] = Option(NodeTask.getSimpleNodeTrace(t))
    def getNodeClassTrace: Option[List[StackTraceElement]] =
      Option(NodeTask.getNodeClassTrace(t)).map(_.asScala.toList)
  }

  implicit final class SeqHelper[T](val t: TraversableOnce[T]) extends AnyVal {
    def toOptimusSeq: OptimusSeq[T] = OptimusSeq.from(t)
  }
  implicit final class DoubleSeqHelper(val t: TraversableOnce[Double]) extends AnyVal {
    def toOptimusDoubleSeq: OptimusDoubleSeq = OptimusDoubleSeq.from(t)
  }
}
