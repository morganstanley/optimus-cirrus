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

import java.lang.reflect.Method
import java.util.Arrays

import optimus.graph.Node
import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform._
import optimus.platform.relational.ExtensionProxyFactory
import optimus.platform.relational._
import optimus.platform.relational.aggregation._
import optimus.platform.relational.internal.RelationalUtils
import optimus.platform.relational.tree.QueryMethod
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.tree.TypeInfoUtils

import scala.collection.mutable
import optimus.scalacompat.collection._
import scala.collection.compat._

object IterableProvider {

  type NodeOrFunc[T, R] = Either[T => Node[R], T => R]

  def mapSync(src: Iterable[Any], f: Any => Any): Iterable[Any] = {
    src.map(f)
  }

  @node def map(src: Iterable[Any], f: Any => Node[Any]): Iterable[Any] = {
    src.apar(PriqlSettings.concurrencyLevel).mapPrelifted(f)
  }

  @node def mapNodeorFunc(src: Iterable[Any], nodeOrFunc: NodeOrFunc[Any, Any]): Iterable[Any] = {
    nodeOrFunc match {
      case Left(nf) => src.apar(PriqlSettings.concurrencyLevel).mapPrelifted(nf)
      case Right(f) => src.map(f)
    }
  }

  @node def flatMap(
      src: Iterable[Any],
      nodeOrFunc: NodeOrFunc[Any, Query[Any]],
      fromIterable: Boolean = false): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val subQueries: Seq[Query[Any]] = nodeOrFunc match {
        case Left(nf) => src.apar(PriqlSettings.concurrencyLevel).mapPrelifted(nf)(collection.Seq.breakOut)
        case Right(f) => src.iterator.map(f).toIndexedSeq
      }
      if (fromIterable) {
        subQueries.flatMap { q =>
          q.element match { case x: ScalaTypeMultiRelation[_] => x.getSync() }
        }
      } else {
        val Seq(x, xs @ _*) = subQueries
        val union = x.union(xs: _*)
        union.provider.execute[Iterable[Any]](union.element).apply()
      }
    }
  }

  def groupBySync(src: Iterable[Any], f: Any => Any, conv: Iterable[Any] => Query[Any]): Iterable[Any] = {
    src.groupBy(t => f(t)).mapValuesNow(conv)
  }

  @node def groupBy(src: Iterable[Any], nf: Any => Node[Any], conv: Iterable[Any] => Query[Any]): Iterable[Any] = {
    val fn = asNode.apply$withNode(nf)
    src.apar(PriqlSettings.concurrencyLevel).groupBy(t => fn(t)).mapValuesNow(conv)
  }

  def groupByTypedSync(
      src: Iterable[Any],
      valuesVerbatim: Boolean,
      keyTypeInfo: TypeInfo[_],
      valueTypeInfo: TypeInfo[_],
      conv: Iterable[Any] => Query[Any]): Iterable[Any] = {
    val keyFactory = ExtensionProxyFactory(keyTypeInfo, TypeInfo.noInfo)
    val valueFactory = if (valuesVerbatim) null else ExtensionProxyFactory(valueTypeInfo, TypeInfo.noInfo)
    val proxyData = src.map(item => {
      val keyProxy = keyFactory.proxy(item.asInstanceOf[AnyRef], null)
      val valueProxy = if (valuesVerbatim) item else valueFactory.proxy(item.asInstanceOf[AnyRef], null)
      (keyProxy, valueProxy)
    })
    groupByProxyData(proxyData).map { case (key, data) =>
      (key, conv(data))
    }
  }

  private def groupByProxyData(data: Iterable[(Any, Any)]): Map[Any, Iterable[Any]] = {
    import scala.collection.mutable

    val m = mutable.Map.empty[Any, mutable.ArrayBuffer[Any]]
    for ((key, value) <- data) {
      val buf = m.getOrElseUpdate(key, new mutable.ArrayBuffer[Any])
      buf += value
    }
    val b = Map.newBuilder[Any, Iterable[Any]]
    for ((k, v) <- m) b += ((k, v.result()))
    b.result()
  }

  def mapValuesSync(src: Iterable[Any], f: Any => Any): Iterable[Any] = {
    if (src.isEmpty) src
    else
      src match {
        case x: Iterable[_] => x.map { case (k, v) => (k, f(v)) }
      }
  }

  @node def mapValues(src: Iterable[Any], nf: Any => Node[Any]): Iterable[Any] = {
    if (src.isEmpty) src
    else
      src match {
        case x: Iterable[_] =>
          val fn = asNode.apply$withNode(nf)
          x.apar(PriqlSettings.concurrencyLevel).map { case (k, v) => (k, fn(v)) }
      }
  }

  @node
  def aggregateByImplicit(
      src: Iterable[Any],
      groupKeyType: TypeInfo[_],
      aggregatorsWithType: List[(NodeFunction1[Any, Any], TypeInfo[_])]): Iterable[Any] = {

    val originBuildProxy = (groupKey: AnyRef, aggregateValue: AnyRef) => {
      val proxyFactory = ExtensionProxyFactory(groupKeyType, aggregatorsWithType.head._2, false)
      proxyFactory.proxy(groupKey, aggregateValue)
    }
    val originProxyType = TypeInfo.intersect(groupKeyType, aggregatorsWithType.head._2).asInstanceOf[TypeInfo[Any]]
    val nodeFunc = aggregatorsWithType.head._1

    if (src.isEmpty) src
    else
      src match {
        case x: Map[_, _] =>
          x.apar(PriqlSettings.concurrencyLevel)
            .map {
              case (k: AnyRef, v: Query[Any]) => {
                val originalValue = originBuildProxy(k, nodeFunc(v).asInstanceOf[AnyRef])
                val res = aggregatorsWithType.tail
                  .foldLeft((originalValue, originProxyType))((valueWithType, funcWithType) => {
                    val proxyFunc = (previousValue: AnyRef, aggregateValue: AnyRef) => {
                      val proxyFactory = ExtensionProxyFactory(valueWithType._2, funcWithType._2, false)
                      proxyFactory.proxy(previousValue, aggregateValue)
                    }
                    val currentValue = proxyFunc(valueWithType._1, funcWithType._1(v).asInstanceOf[AnyRef])
                    val currentType = TypeInfo.intersect(valueWithType._2, funcWithType._2.asInstanceOf[TypeInfo[Any]])
                    (currentValue, currentType)
                  })
                res._1
              }
              case x => throw new IllegalArgumentException(s"unexpected tuple $x")
            }(Iterable.breakOut)
      }
  }

  def aggregateBySync(src: Iterable[Any], groupKeyType: TypeInfo[_], aggregateType: TypeInfo[_]): Iterable[Any] = {
    val proxyFactory = ExtensionProxyFactory(groupKeyType, aggregateType, false)
    src.map { case (groupKey: AnyRef, aggregateValue: AnyRef) => proxyFactory.proxy(groupKey, aggregateValue) }
  }

  def aggregateByUntypedSync(
      src: Iterable[DynamicObject],
      groupByProperties: Set[String],
      untypedAggregations: Seq[UntypedAggregation]): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val groupKeys = groupByProperties.toSeq.sorted
      val keyValues = src.map { d =>
        val arr: Array[Any] = groupKeys.iterator.map(d.get).toArray
        new MultiKey(arr) -> d
      }
      val grouped = mutable.LinkedHashMap.empty[MultiKey, mutable.ArrayBuffer[DynamicObject]]
      for ((key, value) <- keyValues) {
        val buf = grouped.getOrElseUpdate(key, new mutable.ArrayBuffer[DynamicObject])
        buf += value
      }
      grouped.map { case (multiKey, value) =>
        val map: mutable.Map[String, Any] = groupKeys.iterator.zip(multiKey.keys.iterator).convertTo(mutable.Map)
        untypedAggregations.foreach { agg =>
          map.put(agg.aggregateColumn, Aggregator.aggregate(value.flatMap(agg.read), agg.aggregator))
        }
        new MapBasedDynamicObject(map.toMap)
      }
    }
  }

  def filterSync(src: Iterable[Any], p: Any => Boolean): Iterable[Any] = {
    src.filter(p)
  }

  @node def filter(src: Iterable[Any], p: Any => Node[Boolean]): Iterable[Any] = {
    src.apar(PriqlSettings.concurrencyLevel).filterPrelifted(p)
  }

  def extendTypedSync(src: Iterable[Any], f: Any => Any)(srcType: TypeInfo[_], extType: TypeInfo[_]): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val factory = ExtensionProxyFactory(srcType, extType, false)
      src.map(t => {
        val (row: AnyRef, ext: AnyRef) = (t, f(t))
        factory.proxy(row, ext)
      })
    }
  }

  @node def extendTyped(src: Iterable[Any], f: Any => Node[Any])(
      srcType: TypeInfo[_],
      extType: TypeInfo[_]): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val factory = ExtensionProxyFactory(srcType, extType, false)
      val fn = asNode.apply$withNode(f)
      src
        .apar(PriqlSettings.concurrencyLevel)
        .map(t => {
          val (row: AnyRef, ext: AnyRef) = (t, fn(t))
          factory.proxy(row, ext)
        })
    }
  }

  def replaceSync(src: Iterable[Any], f: Any => Any)(srcType: TypeInfo[_], extType: TypeInfo[_]): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val factory = ExtensionProxyFactory(srcType, extType, true)
      src.map(t => {
        val (row: AnyRef, ext: AnyRef) = (t, f(t))
        factory.proxy(row, ext)
      })
    }
  }

  @node def replace(src: Iterable[Any], f: Any => Node[Any])(
      srcType: TypeInfo[_],
      extType: TypeInfo[_]): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val factory = ExtensionProxyFactory(srcType, extType, true)
      val fn = asNode.apply$withNode(f)
      src
        .apar(PriqlSettings.concurrencyLevel)
        .map(t => {
          val (row: AnyRef, ext: AnyRef) = (t, fn(t))
          factory.proxy(row, ext)
        })
    }
  }

  def extendTypedValueSync(src: Iterable[Any], extVals: List[(Any, TypeInfo[_])])(
      srcType: TypeInfo[_]): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val ((v1, vt1) :: otherExtVals) = extVals
      val (proxyVal, proxyType) = otherExtVals.foldLeft((v1, vt1.cast[Any]))((acc, next) => {
        val (v: AnyRef, vt) = next
        val (accVal: AnyRef, accType) = acc

        val factory = ExtensionProxyFactory(accType, vt, false)
        val proxyType = TypeInfo.intersect(accType, vt).cast[Any]
        (factory.proxy(accVal, v), proxyType)
      })

      val factory = ExtensionProxyFactory(srcType, proxyType, false)
      src.map(t => {
        val (row: AnyRef, ext: AnyRef) = (t, proxyVal)
        factory.proxy(row, ext)
      })
    }
  }

  def replaceValueSync(src: Iterable[Any], extVals: List[(Any, TypeInfo[_])])(srcType: TypeInfo[_]): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val ((v1, vt1) :: otherExtVals) = extVals
      val (proxyVal, proxyType) = otherExtVals.foldLeft((v1, vt1.cast[Any]))((acc, next) => {
        val (v: AnyRef, vt) = next
        val (accVal: AnyRef, accType) = acc

        val factory = ExtensionProxyFactory(accType, vt, true)
        val proxyType = TypeInfo.intersect(accType, vt).cast[Any]
        (factory.proxy(accVal, v), proxyType)
      })

      val factory = ExtensionProxyFactory(srcType, proxyType, true)
      src.map(t => {
        val (row: AnyRef, ext: AnyRef) = (t, proxyVal)
        factory.proxy(row, ext)
      })
    }
  }

  @node def sortBy(src: Iterable[Any], nf: Any => Node[Any], ordering: Ordering[_]): Iterable[Any] = {
    val nodeFunction1 = asNode.apply$withNode[Any, Any](nf)
    val r: Array[(Any, Any)] = src.apar(PriqlSettings.concurrencyLevel).map(t => (nodeFunction1(t), t))(Array.breakOut)
    Arrays.sort(r, ordering.asInstanceOf[Ordering[Any]].on((t: (Any, Any)) => t._1))
    r.iterator.map(_._2).toSeq
  }

  def sortBySync(src: Iterable[Any], f: Any => Any, ordering: Ordering[_]): Iterable[Any] =
    src.toSeq.sortBy(f)(ordering.asInstanceOf[Ordering[Any]])

  def shapeToUntypeSync(src: Iterable[Any], f: Any => Any, shapeToType: TypeInfo[_]): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val shapeClassColumnInfo: Map[String, Method] = {
        val shapeClass = shapeToType.clazz
        val getters = RelationalUtils.findAllGetterMethodFromJava(shapeClass)
        if (getters.isEmpty)
          TypeInfoUtils.findAllPropertyMethod(shapeClass, true)
        else getters
      }

      src.map(item => {
        val shapeItem = f(item)
        val mutableObj = item match { case dynamic: DynamicObject => MutableDynamicObject(dynamic) }
        shapeClassColumnInfo.foreach {
          case (name: String, method: Method) => {
            val res = method.invoke(shapeItem)
            // there may be some duplicated column between rawClass and shapeClass, if so shapeClass will override the rawClass value
            mutableObj.put(name, res)
          }
        }
        mutableObj
      })
    }
  }

  @node def shapeToUntype(src: Iterable[Any], nf: Any => Node[Any], shapeToType: TypeInfo[_]): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val shapeClassColumnInfo: Map[String, Method] = {
        val shapeClass = shapeToType.clazz
        val getters = RelationalUtils.findAllGetterMethodFromJava(shapeClass)
        if (getters.isEmpty)
          TypeInfoUtils.findAllPropertyMethod(shapeClass, true)
        else getters
      }

      val nodeFunction1 = asNode.apply$withNode[Any, Any](nf)
      src
        .apar(PriqlSettings.concurrencyLevel)
        .map(item => {
          val shapeItem = nodeFunction1(item)
          val mutableObj = item match { case dynamic: DynamicObject => MutableDynamicObject(dynamic) }
          shapeClassColumnInfo.foreach {
            case (name: String, method: Method) => {
              val res = method.invoke(shapeItem)
              // there may be some duplicated column between rawClass and shapeClass, if so shapeClass will override the rawClass value
              mutableObj.put(name, res)
            }
          }
          mutableObj
        })
    }
  }

  def extendSync(src: Iterable[Any], f: Any => Any, field: String): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      src.map(item => {
        val mutableObj = item match { case dynamic: DynamicObject => MutableDynamicObject(dynamic) }
        mutableObj.put(field, f(item))
        mutableObj
      })
    }
  }

  @node def extend(src: Iterable[Any], nf: Any => Node[Any], field: String): Iterable[Any] = {
    if (src.isEmpty) src
    else {
      val nodeFunction1 = asNode.apply$withNode[Any, Any](nf)
      src
        .apar(PriqlSettings.concurrencyLevel)
        .map(item => {
          val mutableObj = item match { case dynamic: DynamicObject => MutableDynamicObject(dynamic) }
          mutableObj.put(field, nodeFunction1(item))
          mutableObj
        })
    }
  }

  @node def union(nodeFuncList: List[NodeFunction0[Iterable[Any]]]): Iterable[Any] = {
    nodeFuncList.apar(PriqlSettings.concurrencyLevel).flatMap(f => f())
  }

  def mergeSync(
      items: Iterable[Any],
      groupKey: RelationKey[Any],
      itemType: TypeInfo[_],
      ag: Aggregator[Any]): Iterable[Any] = {
    val aggType = ag.resultType
    val proxyFactory = ExtensionProxyFactory(itemType, aggType, true)
    items.groupBy(i => groupKey.ofSync(i)).map { case (_, v) =>
      proxyFactory.proxy(v.head.asInstanceOf[AnyRef], Aggregator.aggregate(v, ag).asInstanceOf[AnyRef])
    }
  }

  @node def merge(
      items: Iterable[Any],
      groupKey: RelationKey[Any],
      itemType: TypeInfo[_],
      ag: Aggregator[Any]): Iterable[Any] = {
    val aggType = ag.resultType
    val proxyFactory = ExtensionProxyFactory(itemType, aggType, true)
    items.apar.groupBy(i => groupKey.of(i)).map { case (_, v) =>
      proxyFactory.proxy(v.head.asInstanceOf[AnyRef], Aggregator.aggregate(v, ag).asInstanceOf[AnyRef])
    }
  }

  @node def difference(left: Iterable[Any], right: Iterable[Any], key: RelationKey[Any]): Iterable[Any] = {
    if (left.isEmpty || right.isEmpty) left
    else {
      if ((key == null) || key.fields.isEmpty) left.toVector.diff(right.toVector)
      else {
        val res = if (key.isSyncSafe) {
          val rightKeys: Set[Any] = right.iterator.map(t => key.ofSync(t)).toSet
          left.filterNot(value => rightKeys.contains(key.ofSync(value)))
        } else {
          val rightKeys: Set[Any] = right.apar(PriqlSettings.concurrencyLevel).map(t => key.of(t))(Set.breakOut)
          left.apar(PriqlSettings.concurrencyLevel).filter(value => rightKeys.contains(key.of(value)) == false)
        }
        res
      }
    }
  }

  @node def arrange(src: Iterable[Any], key: RelationKey[Any]): Iterable[Any] = {
    ArrangeHelper.arrange(src, key)
  }

  def take(src: Iterable[Any], offset: Int, numRows: Int): Iterable[Any] = {
    if (offset < 0)
      // take last
      src.takeRight(numRows)
    else
      src.slice(offset, offset + numRows)
  }

  @node def joinOnLambda(
      left: Iterable[Any],
      right: Iterable[Any],
      queryMethod: QueryMethod,
      onLambda: Either[(Any, Any) => Node[Boolean], (Any, Any) => Boolean],
      leftFunc: NodeOrFunc[Any, Any],
      rightFunc: NodeOrFunc[Any, Any]): Iterable[Any] = {
    JoinHelper.doLambdaJoin(left, right, onLambda, queryMethod, leftFunc, rightFunc)
  }

  @node def joinOnSelectKey(
      leftDataMap: Iterable[(Any, MultiKey)],
      rightDataMap: Iterable[(Any, MultiKey)],
      queryMethod: QueryMethod,
      leftFunc: NodeOrFunc[Any, Any],
      rightFunc: NodeOrFunc[Any, Any]): Iterable[Any] = {
    JoinHelper.doSelectKeyJoin(leftDataMap, rightDataMap, queryMethod, leftFunc, rightFunc)
  }

  @node def joinOnSelectKeyNatural(
      leftDataMap: Iterable[(Any, MultiKey)],
      rightDataMap: Iterable[(Any, MultiKey)],
      queryMethod: QueryMethod,
      leftFunc: NodeOrFunc[Any, Any],
      rightFunc: NodeOrFunc[Any, Any],
      sideAProxyLambda: (AnyRef, AnyRef) => AnyRef,
      sideBProxyLambda: (AnyRef, AnyRef) => AnyRef): Iterable[Any] = {
    JoinHelper.doSelectKeyJoinNatural(
      leftDataMap,
      rightDataMap,
      queryMethod,
      leftFunc,
      rightFunc,
      sideAProxyLambda,
      sideBProxyLambda)
  }

  /**
   * remove duplicated row based on priql key while keeping relative order unchanged, if two rows have the same key but
   * different field values then throwing an exception.
   */
  @node def distinct(src: Iterable[Any], key: RelationKey[Any]): Iterable[Any] = {
    val size = src.size
    if (size <= 1) src
    else {
      val indices: Array[Int] = Array.tabulate(size)(idx => idx)
      removeDuplicatedRowsBasedOnKey(indices, src, key, size)
    }
  }

  def aggregateSync(src: Iterable[Any], aggr: Aggregator[Any]): Any = Aggregator.aggregate(src, aggr)

  def aggregateSync(src: Iterable[Any], f: Any => Any, aggr: Aggregator[Any]): Any =
    Aggregator.aggregate(src.map(f), aggr)

  @node def aggregate(src: Iterable[Any], nf: Any => Node[Any], aggr: Aggregator[Any]): Any = {
    val middleResult = src.apar(PriqlSettings.concurrencyLevel).mapPrelifted(nf)
    Aggregator.aggregate(middleResult, aggr)
  }

  /**
   * indices: Array[Int] record the original order of data
   */
  @node
  private[optimus] def removeDuplicatedRowsBasedOnKey(
      indices: Array[Int],
      src: Iterable[Any],
      key: RelationKey[Any],
      length: Int): Iterable[Any] = {
    def sort(primary: Array[Int], secondary: Array[Int]) = {
      def exchange(i: Int, j: Int): Unit = {
        val temp1 = primary(i)
        primary(i) = primary(j)
        primary(j) = temp1

        val temp2 = secondary(i)
        secondary(i) = secondary(j)
        secondary(j) = temp2
      }

      def quicksort(low: Int, high: Int): Unit = {
        var i = low
        var j = high
        val midIndex = low + (high - low) / 2
        val pivot1 = primary(midIndex)
        val pivot2 = secondary(midIndex)

        while (i <= j) {
          while (primary(i) < pivot1 || (primary(i) == pivot1 && secondary(i) < pivot2)) i += 1
          while (primary(j) > pivot1 || (primary(j) == pivot1 && secondary(j) > pivot2)) j -= 1
          if (i <= j) {
            exchange(i, j)
            i += 1
            j -= 1
          }
        }
        if (low < j)
          quicksort(low, j)
        if (i < high)
          quicksort(i, high)
      }

      if (primary.length != 0)
        quicksort(0, primary.length - 1)
    }

    val data = src.toArray
    val hashCodeArray: Array[Int] = ArrangeHelper.getKeyHashCodes(src, key)

    sort(hashCodeArray, indices)

    var from = 0
    val dupIndexList = new mutable.ListBuffer[(Int, Int)]

    while (from < length) {
      var to = from
      val hashCode = hashCodeArray(from)
      while (to < length - 1 && hashCodeArray(to + 1) == hashCode) to += 1
      if (to > from) {
        // Currently aync plugin can't handle @node call in this "if {}" properly, especially when data is large.
        // So we just store (from, to) in dupIndexList and sort key later.
        dupIndexList += ((from, to))
      }
      from = to + 1
    }

    val duplicateCount = dupIndexList.apar.map { case (from: Int, to: Int) =>
      collapse(indices, data, key, length, from, to + 1)
    } sum

    if (duplicateCount == 0) src
    else {
      val resultSize = length - duplicateCount
      Arrays.sort(indices)
      val buf = new mutable.ArrayBuffer[Any](resultSize)
      var pos = 0
      while (pos < resultSize) {
        buf += data(indices(pos))
        pos += 1
      }
      buf.result()
    }
  }

  /**
   * the range (from, until) means all rows have the key hash code but key may be different, we should pick out all rows
   * with the same key and among those rows, pick up the smallest index and make the remaining removed (assign all other
   * rows' index to be infinity) we need distinct to be stable e.g map operator after sort should honour sort operator's
   * output order
   */
  @node private[optimus] def collapse(
      indices: Array[Int],
      data: Array[Any],
      key: RelationKey[Any],
      length: Int,
      from: Int,
      until: Int): Int = {
    def checkUniqueness(v: Vector[Any]): Unit = {
      val len = v.length
      var i = 0
      while (i < len) {
        val k = v(i)
        var j = i + 1
        while (j < len) {
          if (v(j) == k)
            throw new IllegalArgumentException(s"Duplicate key: Two unequal rows have the same key. Row 1: ${data(
                indices(from + i))}; Row 2: ${data(indices(from + j))}; Key: $k; Key fields: ${key.fields.toList}")
          j += 1
        }
        i += 1
      }
    }

    var duplicateCount = 0
    var i = from + 1
    var u = until
    while (i < u) {
      val item = data(indices(i))
      val index = indices.slice(from, i).indexWhere(t => RelationalUtils.equal(data(t), item))
      if (index != -1) {
        u -= 1
        duplicateCount += 1
        indices(i) = indices(u)
        indices(u) = length
      } else {
        i += 1
      }
    }
    if (u - from > 1) {
      val keys: Vector[Any] =
        if (key.isSyncSafe)
          indices.slice(from, u).iterator.map(v => key.ofSync(data(v))).toVector
        else
          indices.toSeq.slice(from, u).apar(PriqlSettings.concurrencyLevel).map(v => key.of(data(v)))(Vector.breakOut)
      checkUniqueness(keys)
    }
    duplicateCount
  }
}
