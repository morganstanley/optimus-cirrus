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
package optimus.platform.relational.execution

import optimus.platform._
import optimus.platform.relational.{RelationColumnView, RelationalColumnarUnsupportException, RelationalException}
import optimus.platform.relational.internal.RelationalUtils
import optimus.platform.relational.internal.OptimusCoreAPI.asyncResult
import optimus.platform.relational.tree.{ElementType, MethodPosition, MultiRelationElement, TypeInfo}
import optimus.platform.util.DependencyTransformer

object CanBuildIterable {
  private[platform] object DefaultBuilder extends DependencyTransformer[CanBuildIterable, Iterable[Any]] {
    override protected def dependenciesOverride(f: CanBuildIterable): IndexedSeq[CanBuildIterable] = {
      f.dependencies
    }

    @node override protected def transformOverride(f: CanBuildIterable, s: IndexedSeq[Iterable[Any]]): Iterable[Any] = {
      f.buildIterableOverride(s)
    }
  }

  @node protected def buildIterableOverride(c: CanBuildIterable, d: IndexedSeq[Iterable[Any]]): Iterable[Any] =
    c.buildIterableOverride(d)
  def dependencies(c: CanBuildIterable) = c.dependencies
}

/**
 * Extended by IterativeExecNode to be used with DependencyTransformer
 *
 * When we createIterable, the same ExecNode will only create one result (this logic is also in DependencyTransformer)
 */
trait CanBuildIterable {
  self: IterativeExecNode[_] =>

  @node protected def buildIterableOverride(dependencies: IndexedSeq[Iterable[Any]]): Iterable[Any]

  protected def dependencies: IndexedSeq[CanBuildIterable] = Vector.empty
}

case object RuntimeExec extends ElementType

/**
 * Every node in the execution tree is a kind of IterativeExecNode.
 */
abstract class IterativeExecNode[IRowType](
    override val rowTypeInfo: TypeInfo[IRowType],
    keys: RelationKey[_],
    pos: MethodPosition)
    extends MultiRelationElement(RuntimeExec, rowTypeInfo, keys, pos)
    with CanBuildIterable {

  override def projectedType(): TypeInfo[IRowType] = rowTypeInfo

  protected def shouldExecuteDistinct = true

  def isDistinct = RelationalUtils.hasKeys(key) && shouldExecuteDistinct

  @node protected def buildIterableOverride(dependencies: IndexedSeq[Iterable[Any]]): Iterable[Any] = {
    val data: Iterable[IRowType] = doCreateIterable(dependencies)
    if (isDistinct) distinctDataWithKeys(data, key.asInstanceOf[RelationKey[IRowType]]) else data
  }

  @async private def distinctDataWithKeys(data: Iterable[IRowType], key: RelationKey[IRowType]): Iterable[IRowType] = {
    val res = asyncResult {
      RelationalUtils.distinctDataWithKeys(data, key)
    }
    addPosForException(res, pos)
  }

  /**
   * For different kinds of execution nodes, the doCreateIterable function includes all the execution logic for this
   * kind of node and returns a array, which contains the result data.
   */
  @node protected def doCreateIterable(dependencies: IndexedSeq[Iterable[_]]): Iterable[IRowType]

  @node final def createIterable: Iterable[IRowType] = {
    CanBuildIterable.DefaultBuilder.transform(this).asInstanceOf[Iterable[IRowType]]
  }

  /**
   * for backward compatibility use blank implementation instead of abstract method
   */
  protected def doCreateColumns(): RelationColumnView = {
    throw new RelationalColumnarUnsupportException("please implement doCreateColumns for this ExecNode")
  }

  def createColumns: RelationColumnView = {
    val columns = doCreateColumns()
    if (RelationalUtils.hasKeys(keys) && shouldExecuteDistinct)
      RelationalUtils.distinctColumnBasedDataWithKeys(columns, keys)
    else columns
  }

  def keyIsWeakerThan(otherKey: RelationKey[_]): Boolean = {
    if (key == otherKey) false // quick check: if the keys are the same, we're certainly not weaker
    else if (!RelationalUtils.hasKeys(key))
      false // if we have no key, every row is considered distinct, so we are at least as strict as the otherKey
    else if (!RelationalUtils.hasKeys(otherKey))
      true // if we have a key but the otherKey doesn't, we can't tell if we're stricter or not; we might be weaker
    else {
      // ok, we both have keys, so do we fully cover the other key's fields? if not then we are weaker
      val keySet = key.fields.toSet
      val otherKeySet = otherKey.fields
      val isMissingField = otherKeySet.exists(!keySet.contains(_))
      isMissingField
    }
  }

  /**
   * Get result value or throw exception with method position information if there is exception.
   */
  final def addPosForException(res: NodeResult[Iterable[IRowType]], pos: MethodPosition): Iterable[IRowType] = {
    if (res.hasException) {
      val ex = res.exception
      if (pos != null) {
        ex match {
          case exception: IllegalArgumentException =>
            throw new IllegalArgumentException(
              exception.getMessage + "\n\tFailed PriQL Operation Position: " + this.pos.posInfo,
              exception)
          case exception: NullPointerException =>
            throw new NullPointerException(
              exception.getMessage + "\n\tFailed PriQL Operation Position: " + this.pos.posInfo)
          case exception: RelationalException =>
            throw new RelationalException(
              exception.getMessage + "\n\tFailed PriQL Operation Position: " + this.pos.posInfo,
              exception)
          case _ => throw ex
        }
      } else throw ex
    } else res.value
  }
}
