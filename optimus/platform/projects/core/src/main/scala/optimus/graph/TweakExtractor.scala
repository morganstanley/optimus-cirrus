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
package optimus.graph
import optimus.platform.EvaluationQueue
import optimus.platform.ScenarioStack

trait TweakKeyExtractor extends Serializable {
  def key(node: NodeKey[_]): Any
}

trait SITweakValueProvider extends Serializable {
  def isKeyDependent: Boolean = true
  def valueOf(node: PropertyNode[_]): Any
  def serializeAsValue: Option[Any] = None
}

final class TweakValueProviderNode[T](
    val valueProvider: SITweakValueProvider,
    key: PropertyNode[T],
    val modify: Boolean)
    extends CompletableNode[T] {

  def this(valueProvider: SITweakValueProvider, modify: Boolean) = this(valueProvider, null, modify)

  override def hashCode(): Int = valueProvider.hashCode()
  override def equals(obj: Any): Boolean = obj match {
    case that: TweakValueProviderNode[_] => valueProvider == that.valueProvider // source node not available yet
    case _                               => false
  }

  def isKeyDependent: Boolean = valueProvider.isKeyDependent

  def copyWith(pn: PropertyNode[T], computeSS: ScenarioStack): Node[T] = {
    val tvpNode = new TweakValueProviderNode[T](valueProvider, pn, modify)
    if (modify) tvpNode.attach(computeSS) // attach the ss for previous value of pn
    else tvpNode.attach(computeSS.siRoot)
    tvpNode
  }

  override def run(ec: OGSchedulerContext): Unit = {
    if (modify) {
      val srcNode = scenarioStack().getNode(key, ec) // retrieve the scenario stack and run the node
      ec.enqueue(srcNode)
      srcNode.continueWith(this, ec) // callback onChildCompleted
    } else {
      val valueProvided = valueProvider.valueOf(key)
      completeWithResult(valueProvided.asInstanceOf[T], ec)
    }
  }

  override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    // retrieve the previous value from child node and use it as extractor value
    val valueProvided = valueProvider.valueOf(child.asInstanceOf[PropertyNode[_]])
    completeWithResult(valueProvided.asInstanceOf[T], eq)
  }

  // noinspection ScalaUnusedSymbol
  // For more info on writeReplace
  // see https://docs.oracle.com/en/java/javase/11/docs/specs/serialization/output.html#the-writereplace-method
  private def writeReplace(): AnyRef = {
    valueProvider.serializeAsValue match {
      case Some(value) =>
        // there's a race and some other thread may run us first
        if ((tryRun() & NodeTask.STATE_NOT_RUNNABLE) == 0) {
          replace(ScenarioStack.constant)
          completeWithResult(value.asInstanceOf[T], Scheduler.currentOrDefault)
        }
        new AlreadyCompletedNode(value)
      case None => this
    }
  }
}
