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
package optimus.platform.relational.inmemory

import optimus.graph.Node
import optimus.platform._
import optimus.platform.relational.RelationalException
import optimus.platform.relational.internal.RelationalTreeUtils
import optimus.platform.relational.tree._
import optimus.platform._
import com.google.common.collect.ArrayListMultimap
import optimus.platform.relational.inmemory.IterableProvider.NodeOrFunc

import scala.collection.mutable.ArrayBuffer

object JoinHelper {
  import optimus.platform.AsyncImplicits._

  def getRightDefaultValueSync(joinRows: Iterable[(Any, Any)], func: Any => Any, l: Any, queryMethod: QueryMethod) = {
    if (joinRows.isEmpty && (queryMethod == QueryMethod.LEFT_OUTER_JOIN || queryMethod == QueryMethod.FULL_OUTER_JOIN))
      List((l, func(l)))
    else joinRows
  }

  @node def getRightDefaultValue(
      joinRows: Iterable[(Any, Any)],
      fn: NodeFunction1[Any, Any],
      l: Any,
      queryMethod: QueryMethod) = {
    if (joinRows.isEmpty && (queryMethod == QueryMethod.LEFT_OUTER_JOIN || queryMethod == QueryMethod.FULL_OUTER_JOIN))
      List((l, fn(l)))
    else joinRows
  }

  @node def doLambdaJoin(
      left: Iterable[Any],
      right: Iterable[Any],
      lambda: Either[(Any, Any) => Node[Boolean], (Any, Any) => Boolean],
      queryMethod: QueryMethod,
      leftFunc: NodeOrFunc[Any, Any],
      rightFunc: NodeOrFunc[Any, Any]): Iterable[(Any, Any)] = {

    val res = (lambda, rightFunc) match {
      case (Left(lLambda), Left(nf)) =>
        val fn = asNode.apply$withNode(nf)
        left.apar.flatMap(l => {
          val joinRows =
            right.apar.mapPrelifted(r => lLambda(l, r)).view.zip(right).collect { case (true, r) => (l, r) }
          getRightDefaultValue(joinRows, fn, l, queryMethod)
        })
      case (Left(lLambda), Right(func)) =>
        left.apar.flatMap(l => {
          val joinRows =
            right.apar.mapPrelifted(r => lLambda(l, r)).view.zip(right).collect { case (true, r) => (l, r) }
          getRightDefaultValueSync(joinRows, func, l, queryMethod)
        })
      case (Right(rLambda), Right(func)) =>
        left.flatMap(l => {
          val joinRows = right.withFilter(r => rLambda(l, r)).map((l, _))
          getRightDefaultValueSync(joinRows, func, l, queryMethod)
        })
      case (Right(rLambda), Left(nf)) =>
        val fn = asNode.apply$withNode(nf)
        left.apar.flatMap(l => {
          val joinRows = right.withFilter(r => rLambda(l, r)).map((l, _))
          getRightDefaultValue(joinRows, fn, l, queryMethod)
        })
    }
    if (queryMethod == QueryMethod.RIGHT_OUTER_JOIN || queryMethod == QueryMethod.FULL_OUTER_JOIN) {
      val joinedRightKeySet: Set[Any] = res.iterator.map(_._2).toSet
      val rightOuter = leftFunc match {
        case Left(nf) =>
          val fn = asNode.apply$withNode(nf)
          right.filter(r => !joinedRightKeySet.contains(r)).apar.map { r =>
            val res = fn(r)
            (res, r)
          }
        case Right(f) =>
          right.filter(r => !joinedRightKeySet.contains(r)).map { r =>
            val res = f(r)
            (res, r)
          }
      }
      res ++ rightOuter
    } else res
  }

  @node def doSelectKeyJoin(
      leftDataMap: Iterable[(Any, MultiKey)],
      rightDataMap: Iterable[(Any, MultiKey)],
      queryMethod: QueryMethod,
      leftFunc: NodeOrFunc[Any, Any],
      rightFunc: NodeOrFunc[Any, Any]): Iterable[Any] = {
    queryMethod match {
      case QueryMethod.INNER_JOIN =>
        doSelectKeyJoinImpl(leftDataMap, rightDataMap, (a: Any, b: Any) => (a, b), None)
      case QueryMethod.LEFT_OUTER_JOIN =>
        doSelectKeyJoinImpl(leftDataMap, rightDataMap, (a: Any, b: Any) => (a, b), Option(rightFunc))
      case QueryMethod.RIGHT_OUTER_JOIN =>
        doSelectKeyJoinImpl(rightDataMap, leftDataMap, (a: Any, b: Any) => (b, a), Option(leftFunc))
      case QueryMethod.FULL_OUTER_JOIN =>
        doSelectKeyJoinImpl(
          leftDataMap,
          rightDataMap,
          (a: Any, b: Any) => (a, b),
          Option(rightFunc),
          Some(leftFunc, (a: Any, b: Any) => (a, b)))
    }
  }

  @node def doSelectKeyJoinNatural(
      leftDataMap: Iterable[(Any, MultiKey)],
      rightDataMap: Iterable[(Any, MultiKey)],
      queryMethod: QueryMethod,
      leftFunc: NodeOrFunc[Any, Any],
      rightFunc: NodeOrFunc[Any, Any],
      sideAProxyLambda: (AnyRef, AnyRef) => AnyRef,
      sideBProxyLambda: (AnyRef, AnyRef) => AnyRef): Iterable[Any] = {
    queryMethod match {
      case QueryMethod.NATURAL_INNER_JOIN =>
        doSelectKeyJoinImpl(
          leftDataMap,
          rightDataMap,
          (a: Any, b: Any) => sideAProxyLambda(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef]),
          None)
      case QueryMethod.NATURAL_LEFT_OUTER_JOIN =>
        doSelectKeyJoinImpl(
          leftDataMap,
          rightDataMap,
          (a: Any, b: Any) => sideAProxyLambda(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef]),
          Option(rightFunc))
      case QueryMethod.NATURAL_RIGHT_OUTER_JOIN =>
        doSelectKeyJoinImpl(
          rightDataMap,
          leftDataMap,
          (a: Any, b: Any) => sideBProxyLambda(b.asInstanceOf[AnyRef], a.asInstanceOf[AnyRef]),
          Option(leftFunc))
      case QueryMethod.NATURAL_FULL_OUTER_JOIN =>
        doSelectKeyJoinImpl(
          leftDataMap,
          rightDataMap,
          (a: Any, b: Any) => sideAProxyLambda(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef]),
          Option(rightFunc),
          Some(leftFunc, (a: Any, b: Any) => sideBProxyLambda(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef]))
        )
    }
  }

  @node private def doSelectKeyJoinImpl[A, B, T](
      sideADataMap: Iterable[(A, MultiKey)],
      sideBDataMap: Iterable[(B, MultiKey)],
      conversionLambda: (A, B) => T,
      sideADefaultLambdaOption: Option[NodeOrFunc[Any, Any]],
      sideBDefaultLambdaOption: Option[(NodeOrFunc[Any, Any], (A, B) => T)] = None): Iterable[T] = {
    import scala.jdk.CollectionConverters._

    val sideBMultiMap = ArrayListMultimap.create[MultiKey, B]()
    sideBDataMap.foreach { case (value, key) => sideBMultiMap.put(key, value) }

    val unjoinedSideBKeys = new java.util.HashSet[MultiKey](sideBMultiMap.keySet)
    val resultBuffer = new ArrayBuffer[T](1)

    def addJoinedRows(lambdaOpt: Option[Any => Any]) = {
      for ((sideARow, sideAKey) <- sideADataMap) {
        val joinedRows = if (sideBMultiMap.containsKey(sideAKey)) {
          val sideBMatches = sideBMultiMap.get(sideAKey)
          unjoinedSideBKeys.remove(sideAKey)
          sideBMatches.asScala.map(b => conversionLambda(sideARow, b))
        } else {
          lambdaOpt.map { lambda =>
            val b = lambda(sideARow).asInstanceOf[B]
            List(conversionLambda(sideARow, b))
          } getOrElse Nil
        }
        resultBuffer ++= joinedRows
      }
    }

    sideADefaultLambdaOption match {
      case Some(Left(sideADefaultLambda)) =>
        val res = sideADataMap.apar.flatMap { case (sideARow, sideAKey) =>
          if (sideBMultiMap.containsKey(sideAKey)) {
            val sideBMatches = sideBMultiMap.get(sideAKey)
            unjoinedSideBKeys.remove(sideAKey)
            val joinedRows = sideBMatches.asScala.map(b => conversionLambda(sideARow, b))
            joinedRows
          } else {
            val fn = asNode.apply$withNode(sideADefaultLambda)
            val b = fn(sideARow).asInstanceOf[B]
            List(conversionLambda(sideARow, b))
          }
        }
        resultBuffer ++= res
      case Some(Right(sideADefaultLambda)) =>
        addJoinedRows(Some(sideADefaultLambda))
      case None => addJoinedRows(None)
    }

    sideBDefaultLambdaOption match {
      case Some((sideBDefaultLambda, sideBConversionLambda)) =>
        val sideBRows = unjoinedSideBKeys.asScala.flatMap(sideBKey => sideBMultiMap.get(sideBKey).asScala)
        val sideBRes = sideBDefaultLambda match {
          case Left(nf) =>
            val fn = asNode.apply$withNode(nf)
            sideBRows.apar.map { sideBRow =>
              val row = fn(sideBRow).asInstanceOf[A]
              sideBConversionLambda(row, sideBRow)
            }
          case Right(f) =>
            sideBRows.map { sideBRow =>
              val row = f(sideBRow).asInstanceOf[A]
              sideBConversionLambda(row, sideBRow)
            }
        }
        resultBuffer ++= sideBRes
      case None =>
    }
    resultBuffer
  }

  def findFuncElement(arguments: List[MethodArg[RelationElement]], name: String): Option[FuncElement] = {
    arguments.collectFirst { case MethodArg(`name`, f: FuncElement) => f }
  }

  def findConstValueElement(arguments: List[MethodArg[RelationElement]], name: String): Option[ConstValueElement] = {
    arguments.collectFirst { case MethodArg(`name`, c: ConstValueElement) => c }
  }

  private val defaultLambda1: Any => Any = (x: Any) => None
  private val defaultLambda2: Any => Any = (x: Any) => null

  def getWithDefaultLambda(
      methodCode: QueryMethod,
      arguments: List[MethodArg[RelationElement]],
      targetType: Either[TypeInfo[_], TypeInfo[_]]): NodeOrFunc[Any, Any] = {
    import QueryMethod._

    def default(t: TypeInfo[_]) = {
      if (t.clazz == classOf[Option[_]]) defaultLambda1
      else defaultLambda2
    }

    targetType match {
      case Left(leftType) =>
        val fOption =
          findFuncElement(arguments, "leftDefault").map(f => FuncElement.convertFuncElementToLambda(f))
        fOption.getOrElse {
          methodCode match {
            case NATURAL_FULL_OUTER_JOIN | NATURAL_RIGHT_OUTER_JOIN =>
              throw new RelationalException(
                "left default lambda must be defined for natural right-outer-join and full-outer-join")
            case _ => Right(default(leftType))
          }
        }
      case Right(rightType) =>
        val fOption =
          findFuncElement(arguments, "rightDefault").map(f => FuncElement.convertFuncElementToLambda(f))
        fOption.getOrElse {
          methodCode match {
            case NATURAL_FULL_OUTER_JOIN | NATURAL_LEFT_OUTER_JOIN =>
              throw new RelationalException(
                "right default lambda must be defined for natural left-outer-join and full-outer-join")
            case _ => Right(default(rightType))
          }
        }
    }
  }

  @node def getDataMapFromMultiKeyLambda(
      left: Iterable[Any],
      right: Iterable[Any],
      leftMultiKeyFunc: Either[Any => Node[MultiKey], Any => MultiKey],
      rightMultiKeyFunc: Either[Any => Node[MultiKey], Any => MultiKey])
      : (Iterable[(Any, MultiKey)], Iterable[(Any, MultiKey)]) = {
    val leftKeys = leftMultiKeyFunc match {
      case Left(l)  => left.apar.mapPrelifted(l)
      case Right(r) => left.map(r)
    }
    val rightKeys = rightMultiKeyFunc match {
      case Left(l)  => right.apar.mapPrelifted(l)
      case Right(r) => right.map(r)
    }
    (left.zip(leftKeys), right.zip(rightKeys))
  }

  @node def getDataMapFromKeyFields(
      left: Iterable[Any],
      right: Iterable[Any],
      leftKey: DynamicKey,
      rightKey: DynamicKey): (Iterable[(Any, MultiKey)], Iterable[(Any, MultiKey)]) = {
    val leftDataMap =
      if (leftKey.isSyncSafe)
        left.map(t => (t, new MultiKey(leftKey.ofSync(t).toArray[Any])))
      else
        left.apar.map(t => (t, new MultiKey(leftKey.of(t).toArray[Any])))

    val rightDataMap =
      if (rightKey.isSyncSafe)
        right.map(t => (t, new MultiKey(rightKey.ofSync(t).toArray[Any])))
      else
        right.apar.map(t => (t, new MultiKey(rightKey.of(t).toArray[Any])))

    (leftDataMap, rightDataMap)
  }

  def getTupleJoinKeys(
      left: MultiRelationElement,
      right: MultiRelationElement,
      queryMethod: QueryMethod): (DynamicKey, DynamicKey) = {
    val leftKey = left.key
    val rightKey = right.key
    if (leftKey.fields.nonEmpty && rightKey.fields.nonEmpty) {
      val commonFields =
        if (RelationalTreeUtils.compareKeys(leftKey, rightKey)) leftKey.fields
        else {
          val intersection = leftKey.fields.intersect(rightKey.fields)
          // if the keys don't match, but at least some fields do match, use those matching fields
          if (intersection.isEmpty)
            throw new RelationalException(s"Joins have different primary key in left [$leftKey] and right [$rightKey]")
          intersection
        }
      val sortedFields = commonFields.sorted
      (DynamicKey(sortedFields, left.projectedType()), DynamicKey(sortedFields, right.projectedType()))
    } else
      throw new RelationalException("Normal join does not have either an 'on' lambda or common PriQL primary key.")
  }

  def getNaturalJoinKeys(
      left: MultiRelationElement,
      right: MultiRelationElement,
      queryMethod: QueryMethod): (DynamicKey, DynamicKey) = {
    val leftType = left.projectedType()
    val rightType = right.projectedType()
    val leftFields = leftType.propertyNames
    val rightFields = rightType.propertyNames
    val commonFields = leftFields.intersect(rightFields)

    // If there are no common columns we cannot perform a natural join.
    if (commonFields.isEmpty)
      throw new RelationalException(
        s"Unable to perform natural join between ${leftType} and ${rightType}. No common columns.")

    val sortedFields = commonFields.sorted
    (DynamicKey(sortedFields, leftType), DynamicKey(sortedFields, rightType))
  }
}
