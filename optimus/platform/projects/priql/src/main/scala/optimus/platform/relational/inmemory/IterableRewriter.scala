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

import msjava.slf4jutils.scalalog
import optimus.graph.CompletableNode
import optimus.graph.Node
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.platform._
import optimus.platform.annotations.nodeSync
import optimus.platform.relational.ExtensionProxyFactory
import optimus.platform.relational.RelationalException
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.ScalaBasedDynamicObjectFactory
import optimus.platform.relational.aggregation._
import optimus.platform.relational.internal.OptimusCoreAPI._
import optimus.platform.relational.tree._

import scala.collection.mutable.HashMap

class IterableRewriter extends QueryTreeVisitor with MultiRelationRewriter {
  import IterableProvider._

  type NodeOrFunc[T, R] = Either[T => Node[R], T => R]

  object Invoke extends ElementType

  case class CollectionInvocationElement(val nf: NodeFunction0[Iterable[Any]])
      extends RelationElement(Invoke, IterableRewriter.iterAny)
  case class ValueInvocationElement(val nf: NodeFunction0[Any]) extends RelationElement(Invoke, TypeInfo.ANY)

  private val visited = new HashMap[RelationElement, RelationElement]
  private var elementCountMap: HashMap[RelationElement, Int] = null

  def rewriteAndCompile[T](e: RelationElement): NodeFunction0[T] = {
    elementCountMap = ReferenceCalculator.calculate(e)
    val nodeFunc = visitElement(e) match {
      case CollectionInvocationElement(nf) => nf
      case ValueInvocationElement(nf)      => nf
    }
    nodeFunc.asInstanceOf[NodeFunction0[T]]
  }

  override def visitElement(element: RelationElement): RelationElement = {
    element match {
      case e: ProviderRelation =>
        val refCount = elementCountMap.get(element).getOrElse(1)
        if (refCount == 1) super.visitElement(element)
        else {
          visited.getOrElseUpdate(
            e, {
              val CollectionInvocationElement(nf) = handleQuerySrc(e)
              val cache = new IterableResultCache(nf, refCount)
              CollectionInvocationElement(asNode { () =>
                cache.apply()
              })
            }
          )
        }

      case e: MethodElement =>
        val refCount = elementCountMap.get(element).getOrElse(1)
        if (refCount == 1) super.visitElement(element)
        else {
          visited.getOrElseUpdate(
            e, {
              val CollectionInvocationElement(nf) = handleMethod(e)
              val cache = new IterableResultCache(nf, refCount)
              CollectionInvocationElement(asNode { () =>
                cache.apply()
              })
            }
          )
        }

      case _ => super.visitElement(element)
    }
  }

  def rewrite(m: MultiRelationElement): NodeFunction0[Iterable[Any]] = {
    val CollectionInvocationElement(nf) = visitElement(m)
    nf
  }

  protected override def handleQuerySrc(element: ProviderRelation): RelationElement = {
    element match {
      case i: IterableSource[_] =>
        val invoke =
          if (i.isSyncSafe)
            CollectionInvocationElement(asNode { () =>
              i.getSync()
            })
          else
            CollectionInvocationElement(asNode { () =>
              i.get()
            })

        if (i.shouldExecuteDistinct) distinctByKey(invoke, element.key) else invoke
      case _ => throw new RelationalUnsupportedException(s"The provider '$element' does not support 'Query.execute'.")
    }
  }

  protected override def handleMethod(method: MethodElement): RelationElement = {
    import QueryMethod._

    method.methodCode match {
      case WHERE                                                             => rewriteWhere(method)
      case OUTPUT                                                            => rewriteSelect(method)
      case FLATMAP                                                           => rewriteFlatMap(method)
      case GROUP_BY                                                          => rewriteGroupBy(method)
      case GROUP_BY_TYPED                                                    => rewriteGroupByTyped(method)
      case GROUP_MAP_VALUES                                                  => rewriteMapValues(method)
      case EXTEND_TYPED                                                      => rewriteExtendTyped(method)
      case REPLACE                                                           => rewriteReplace(method)
      case SORT                                                              => rewriteSort(method)
      case ARRANGE                                                           => rewriteArrange(method)
      case UNTYPE                                                            => rewriteUntype(method)
      case SHAPE                                                             => rewriteShapeTo(method)
      case EXTEND                                                            => rewriteExtend(method)
      case UNION                                                             => rewriteUnion(method)
      case MERGE                                                             => rewriteMerge(method)
      case DIFFERENCE                                                        => rewriteDifference(method)
      case TAKE                                                              => rewriteTake(method)
      case INNER_JOIN | LEFT_OUTER_JOIN | RIGHT_OUTER_JOIN | FULL_OUTER_JOIN => rewriteTupleJoin(method)
      case NATURAL_FULL_OUTER_JOIN | NATURAL_INNER_JOIN | NATURAL_LEFT_OUTER_JOIN | NATURAL_RIGHT_OUTER_JOIN =>
        rewriteNaturalJoin(method)
      case AGGREGATE_BY          => rewriteAggregateBy(method)
      case AGGREGATE_BY_IMPLICIT => rewriteAggregateByImplicit(method)
      case AGGREGATE_BY_UNTYPED  => rewriteAggregateByUntyped(method)
      case TAKE_DISTINCT         => rewriteDistinct(method)
      case TAKE_DISTINCT_BYKEY   => rewriteDistinctByKey(method)
      case PERMIT_TABLE_SCAN     => visitElement(method.methodArgs(0).param)
      case e: Executable         => rewriteExecutable(method, e)
      case _                     => throw new RelationalException("not implement yet")
    }
  }

  protected def rewriteExecutable(m: MethodElement, e: Executable): RelationElement = {
    CollectionInvocationElement(e.execute(m, this))
  }

  private def distinctByKey(invoke: CollectionInvocationElement, key: RelationKey[_]): RelationElement = {
    if (key == null || key == NoKey)
      invoke
    else
      CollectionInvocationElement(asNode { () =>
        distinct(invoke.nf(), key.asInstanceOf[RelationKey[Any]])
      })
  }

  protected def rewriteSelect(m: MethodElement): RelationElement = {
    val (src :: lam :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    val invoke = lam.param match {
      case FuncElement(c: ScalaLambdaCallee[_, _], _, _) =>
        c.lambda.asInstanceOf[NodeOrFunc[Any, Any]] match {
          case Left(nf) =>
            CollectionInvocationElement(asNode { () =>
              map(sf(), nf)
            })
          case Right(f) =>
            CollectionInvocationElement(asNode { () =>
              mapSync(sf(), f)
            })
        }
    }
    distinctByKey(invoke, m.key)
  }

  protected def rewriteFlatMap(m: MethodElement): RelationElement = {
    val (src :: lam :: MethodArg(_, ConstValueElement(conv: AnyRef, _)) :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    val fromIterable = conv eq QueryConverter.IterableConverter
    val invoke = lam.param match {
      case FuncElement(c: ScalaLambdaCallee[_, _], _, _) =>
        val nodeOrFunc = c.lambda.asInstanceOf[NodeOrFunc[Any, Query[Any]]]
        CollectionInvocationElement(asNode { () =>
          flatMap(sf(), nodeOrFunc, fromIterable)
        })
    }
    distinctByKey(invoke, m.key)
  }

  protected def rewriteGroupBy(m: MethodElement): RelationElement = {
    import QueryConverter.IterableConverter

    val (MethodArg(_, src: MultiRelationElement) :: lam :: MethodArg(
      _,
      ConstValueElement(pvd: QueryProvider, _)) :: _) =
      m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src)
    val conv = (i: Iterable[Any]) =>
      IterableConverter.convertUnique(i, src.key.asInstanceOf[RelationKey[Any]], pvd)(
        src.projectedType().cast[Any],
        MethodPosition.unknown)
    lam.param match {
      case FuncElement(c: ScalaLambdaCallee[_, _], _, _) =>
        c.lambda.asInstanceOf[NodeOrFunc[Any, Any]] match {
          case Left(nf) =>
            CollectionInvocationElement(asNode { () =>
              groupBy(sf(), nf, conv)
            })
          case Right(f) =>
            CollectionInvocationElement(asNode { () =>
              groupBySync(sf(), f, conv)
            })
        }
    }
  }

  protected def rewriteGroupByTyped(m: MethodElement): RelationElement = {
    import QueryConverter.IterableConverter

    val (src :: MethodArg(_, ConstValueElement(keyType: TypeInfo[_], _)) :: MethodArg(
      _,
      ConstValueElement(valueType: TypeInfo[_], _)) :: MethodArg(
      _,
      ConstValueElement(valueKey: RelationKey[_], _)) :: MethodArg(_, ConstValueElement(pvd: QueryProvider, _)) :: _) =
      m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    val valuesVerbatim = src.param.rowTypeInfo =:= valueType
    if (keyType.interfaces == Nil || valueType.interfaces == Nil)
      throw new RelationalException("groupBy[GroupKeyType, GroupValueType] could only apply to interface")
    val conv = (i: Iterable[Any]) =>
      IterableConverter.convert(i, valueKey.asInstanceOf[RelationKey[Any]], pvd)(
        valueType.asInstanceOf[TypeInfo[Any]],
        MethodPosition.unknown)
    CollectionInvocationElement(asNode { () =>
      groupByTypedSync(sf(), valuesVerbatim, keyType, valueType, conv)
    })
  }

  protected def rewriteMapValues(m: MethodElement): RelationElement = {
    val (src :: lam :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    lam.param match {
      case FuncElement(c: ScalaLambdaCallee[_, _], _, _) =>
        c.lambda.asInstanceOf[NodeOrFunc[Any, Any]] match {
          case Left(nf) =>
            CollectionInvocationElement(asNode { () =>
              mapValues(sf(), nf)
            })
          case Right(f) =>
            CollectionInvocationElement(asNode { () =>
              mapValuesSync(sf(), f)
            })
        }
    }
  }

  protected def rewriteAggregateBy(m: MethodElement): RelationElement = {
    import QueryConverter.IterableConverter

    val (src :: remainings) = m.methodArgs
    val (ConstValueElement(keyType: TypeInfo[_], _) :: ConstValueElement(
      valueType: TypeInfo[_],
      _) :: ConstValueElement(aggregateType: TypeInfo[_], _) :: ConstValueElement(
      valueKey,
      _) :: (f: FuncElement) :: ConstValueElement(pvd: QueryProvider, _) :: others) =
      remainings.map(_.arg)

    val CollectionInvocationElement(sf) = visitElement(src.param)

    val valuesVerbatim = src.param.rowTypeInfo == valueType
    if (keyType.interfaces == Nil || valueType.interfaces == Nil)
      throw new RelationalException("aggregateBy[GroupKeyType, GroupValueType] could only apply to interface")
    val conv = (i: Iterable[Any]) =>
      IterableConverter.convert(i, valueKey.asInstanceOf[RelationKey[Any]], pvd)(
        valueType.asInstanceOf[TypeInfo[Any]],
        MethodPosition.unknown)
    val CollectionInvocationElement(groupByTypedFunc) = CollectionInvocationElement(asNode { () =>
      groupByTypedSync(sf(), valuesVerbatim, keyType, valueType, conv)
    })

    val CollectionInvocationElement(mapValuesFunc) = f match {
      case FuncElement(c: ScalaLambdaCallee[_, _], _, _) =>
        c.lambda.asInstanceOf[NodeOrFunc[Any, Any]] match {
          case Left(nf) =>
            CollectionInvocationElement(asNode { () =>
              mapValues(groupByTypedFunc(), nf)
            })
          case Right(f) =>
            CollectionInvocationElement(asNode { () =>
              mapValuesSync(groupByTypedFunc(), f)
            })
        }
    }
    CollectionInvocationElement(asNode { () =>
      aggregateBySync(mapValuesFunc(), keyType, aggregateType)
    })
  }

  protected def rewriteAggregateByImplicit(m: MethodElement): RelationElement = {
    import QueryConverter.IterableConverter

    val (src :: remainings) = m.methodArgs
    val (MethodArg(_, ConstValueElement(keyType: TypeInfo[_], _)) :: MethodArg(
      _,
      ConstValueElement(valueType: TypeInfo[_], _)) :: MethodArg(_, ConstValueElement(aggregateType: TypeInfo[_], _)) ::
      MethodArg(_, ConstValueElement(valueKey, _)) :: MethodArg(
        _,
        ConstValueElement(pvd: QueryProvider, _)) :: MethodArg(_, ConstValueElement(aggregators, _)) :: lefts) =
      remainings

    val CollectionInvocationElement(sf) = visitElement(src.param)

    val valuesVerbatim = src.param.rowTypeInfo == valueType
    if (keyType.interfaces == Nil || valueType.interfaces == Nil)
      throw new RelationalException("aggregateBy[GroupKeyType, GroupValueType] could only apply to interface")
    val conv = (i: Iterable[Any]) =>
      IterableConverter.convert(i, valueKey.asInstanceOf[RelationKey[Any]], pvd)(
        valueType.asInstanceOf[TypeInfo[Any]],
        MethodPosition.unknown)
    val CollectionInvocationElement(groupByTypedFunc) = CollectionInvocationElement(asNode { () =>
      groupByTypedSync(sf(), valuesVerbatim, keyType, valueType, conv)
    })

    CollectionInvocationElement(asNode { () =>
      aggregateByImplicit(
        groupByTypedFunc(),
        keyType,
        aggregators.asInstanceOf[List[(NodeFunction1[Any, Any], TypeInfo[_])]])
    })
  }

  protected def rewriteAggregateByUntyped(m: MethodElement): RelationElement = {
    val (src :: groupbys :: aggregations :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)

    val groupByProperties = groupbys.param match {
      case ConstValueElement(properties: Set[String @unchecked], _) => properties
    }
    val untypedAggregations = aggregations.param match {
      case ConstValueElement(builders: Seq[UntypedAggregation @unchecked], _) => builders
    }
    CollectionInvocationElement(asNode { () =>
      aggregateByUntypedSync(sf().asInstanceOf[Iterable[DynamicObject]], groupByProperties, untypedAggregations)
    })
  }

  protected def rewriteWhere(m: MethodElement): RelationElement = {
    val (src :: lam :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    lam.param match {
      case FuncElement(c: ScalaLambdaCallee[_, _], _, _) =>
        c.lambda.asInstanceOf[NodeOrFunc[Any, Boolean]] match {
          case Left(nf) =>
            CollectionInvocationElement(asNode { () =>
              filter(sf(), nf)
            })
          case Right(f) =>
            CollectionInvocationElement(asNode { () =>
              filterSync(sf(), f)
            })
        }
    }
  }

  protected def rewriteTake(m: MethodElement): RelationElement = {
    val (src :: o :: n :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    val (offset, numRows) = (o.param, n.param) match {
      case (ConstValueElement(o: Int, _), ConstValueElement(r: Int, _)) => (o, r)
      case _ => throw new RelationalException("TAKE should have two parameter: offset and value which are both Int")
    }
    CollectionInvocationElement(asNode { () =>
      take(sf(), offset, numRows)
    })
  }

  protected def rewriteExtendTyped(m: MethodElement): RelationElement = {
    val (src :: lamOrExtVals) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    lamOrExtVals match {
      case (lam :: Nil) if lam.name == "f" =>
        lam.param match {
          case FuncElement(c: ScalaLambdaCallee[_, _], _, _) =>
            c.lambda.asInstanceOf[NodeOrFunc[Any, Any]] match {
              case Left(nf) =>
                CollectionInvocationElement(asNode { () =>
                  extendTyped(sf(), nf)(src.param.projectedType(), c.resType)
                })
              case Right(f) =>
                CollectionInvocationElement(asNode { () =>
                  extendTypedSync(sf(), f)(src.param.projectedType(), c.resType)
                })
            }
        }
      case args if args.forall(_.name == "extVal") =>
        val extVals = args.map { case MethodArg(_, ConstValueElement(v, vt)) => (v, vt) }
        CollectionInvocationElement(asNode { () =>
          extendTypedValueSync(sf(), extVals)(src.param.projectedType())
        })
      case vs => throw new MatchError(vs)
    }
  }

  protected def rewriteReplace(m: MethodElement): RelationElement = {
    val (src :: lamOrRepVals) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    val invoke = lamOrRepVals match {
      case (lam :: Nil) if lam.name == "f" =>
        lam.param match {
          case FuncElement(c: ScalaLambdaCallee[_, _], _, _) =>
            c.lambda.asInstanceOf[NodeOrFunc[Any, Any]] match {
              case Left(nf) =>
                CollectionInvocationElement(asNode { () =>
                  replace(sf(), nf)(src.param.projectedType(), c.resType)
                })
              case Right(f) =>
                CollectionInvocationElement(asNode { () =>
                  replaceSync(sf(), f)(src.param.projectedType(), c.resType)
                })
            }
        }
      case args if args.forall(_.name == "repVal") =>
        val extVals = args.map { case MethodArg(_, ConstValueElement(v, vt)) => (v, vt) }
        CollectionInvocationElement(asNode { () =>
          replaceValueSync(sf(), extVals)(src.param.projectedType())
        })
      case vs => throw new MatchError(vs)
    }
    distinctByKey(invoke, m.key)
  }

  protected def rewriteSort(m: MethodElement): RelationElement = {
    val (src :: f :: order :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    (f.param, order.param) match {
      case (FuncElement(c: ScalaLambdaCallee[_, _], _, _), ConstValueElement(ord: Ordering[_], _)) =>
        c.lambda.asInstanceOf[NodeOrFunc[Any, Any]] match {
          case Left(nf) =>
            CollectionInvocationElement(asNode { () =>
              sortBy(sf(), nf, ord)
            })
          case Right(f) =>
            CollectionInvocationElement(asNode { () =>
              sortBySync(sf(), f, ord)
            })
        }
      case x => throw new IllegalArgumentException(s"unexpected input $x")
    }
  }

  protected def rewriteArrange(m: MethodElement): RelationElement = {
    val (src :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    CollectionInvocationElement(asNode { () =>
      val src = sf()
      val ret = arrange(src, m.key.asInstanceOf[RelationKey[Any]])
      ret
    })
  }

  protected def rewriteDistinct(m: MethodElement): RelationElement = {
    val (src :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    CollectionInvocationElement(asNode { () =>
      sf().toIndexedSeq.distinct
    })
  }

  protected def rewriteDistinctByKey(m: MethodElement): RelationElement = {
    if (m.key == null || m.key == NoKey) rewriteDistinct(m)
    else {
      val (src :: _) = m.methodArgs
      visitElement(src.param) match {
        case invoke: CollectionInvocationElement => distinctByKey(invoke, m.key)
      }
    }
  }

  protected def rewriteUntype(m: MethodElement): RelationElement = {
    val (src :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    val invoke = ScalaBasedDynamicObjectFactory(src.param.projectedType().cast[Any]) match {
      case Left(lf) =>
        CollectionInvocationElement(asNode { () =>
          map(sf(), lf)
        })
      case Right(f) =>
        CollectionInvocationElement(asNode { () =>
          mapSync(sf(), f)
        })
    }
    m.key match {
      case dk: DynamicFromStaticKey =>
        dk.sk match {
          case tk: TupleKey[_, _, _, _] if tk.k1.fields.size + tk.k2.fields.size > tk.fields.size =>
            distinctByKey(invoke, m.key)
          case _ => invoke
        }
      case _ => distinctByKey(invoke, m.key)
    }
  }

  protected def rewriteShapeTo(m: MethodElement): RelationElement = {
    if (m.methodArgs.size < 3) rewriteSelect(m)
    else {
      val (src :: lam :: shapeToType :: _) = m.methodArgs
      val CollectionInvocationElement(sf) = visitElement(src.param)
      val invoke = (lam.param, shapeToType.param) match {
        case (FuncElement(c: ScalaLambdaCallee[_, _], _, _), ConstValueElement(shapeToTypeInfo: TypeInfo[_], _)) =>
          c.lambda.asInstanceOf[NodeOrFunc[Any, Any]] match {
            case Left(nf) =>
              CollectionInvocationElement(asNode { () =>
                shapeToUntype(sf(), nf, shapeToTypeInfo)
              })
            case Right(f) =>
              CollectionInvocationElement(asNode { () =>
                shapeToUntypeSync(sf(), f, shapeToTypeInfo)
              })
          }
        case x => throw new IllegalArgumentException(s"unexpected tuple $x")
      }
      distinctByKey(invoke, m.key)
    }
  }

  protected def rewriteExtend(m: MethodElement): RelationElement = {
    val (src :: field :: lam :: _) = m.methodArgs
    val CollectionInvocationElement(sf) = visitElement(src.param)
    val invoke = (lam.param, field.param) match {
      case (FuncElement(c: ScalaLambdaCallee[_, _], _, _), ConstValueElement(fieldName: String, _)) =>
        c.lambda.asInstanceOf[NodeOrFunc[Any, Any]] match {
          case Left(nf) =>
            CollectionInvocationElement(asNode { () =>
              extend(sf(), nf, fieldName)
            })
          case Right(f) =>
            CollectionInvocationElement(asNode { () =>
              extendSync(sf(), f, fieldName)
            })
        }
      case x => throw new IllegalArgumentException(s"unexpected tuple $x")
    }
    m.key match {
      case dk: DynamicExtendedKey if dk.sk.fields.contains(dk.extraField) => distinctByKey(invoke, m.key)
      case _                                                              => invoke
    }
  }

  protected override def handleFuncCall(func: FuncElement): RelationElement = {
    func match {
      case FuncElement(mc: MethodCallee, Nil, ConstValueElement(nodeFunction: NodeFunction0[_], _))
          if mc.method.name == "apply" =>
        ValueInvocationElement(nodeFunction)

      case FuncElement(mc: MethodCallee, _, _) if mc.method.declaringType.clazz == classOf[Query[_]] =>
        mc.method.name match {
          case "sum" | "average" | "min" | "max" | "aggregate" | "variance" | "stddev" =>
            rewriteAggregate(func)
          case "count" => if (func.arguments.size > 1) rewriteAggregate(func) else rewriteSimpleCount(func)
          case "corr"  => rewriteCorrAggregate(func)
          case m       => rewriteAsIterableFunc(func, m)
        }

      case _ => super.handleFuncCall(func)
    }
  }

  protected def rewriteCorrAggregate(f: FuncElement): RelationElement = {
    val CollectionInvocationElement(sf) = visitElement(f.instance)
    val f1 :: f2 :: ag :: _ = f.arguments
    val FuncElement(c1: ScalaLambdaCallee[_, _], _, _) = f1
    val FuncElement(c2: ScalaLambdaCallee[_, _], _, _) = f2
    val ConstValueElement(cor: Aggregator[Any @unchecked], _) = ag
    val fx = (c1.lambda.asInstanceOf[NodeOrFunc[Any, Any]], c2.lambda.asInstanceOf[NodeOrFunc[Any, Any]]) match {
      case (Left(nf1), Left(nf2)) =>
        val f1 = asNode.apply$withNode[Any, Any](nf1)
        val f2 = asNode.apply$withNode[Any, Any](nf2)
        Left(liftNode { t: Any =>
          (f1(t), f2(t))
        })
      case (Left(nf1), Right(f2)) =>
        val f1 = asNode.apply$withNode[Any, Any](nf1)
        Left(liftNode { t: Any =>
          (f1(t), f2(t))
        })
      case (Right(f1), Left(nf2)) =>
        val f2 = asNode.apply$withNode[Any, Any](nf2)
        Left(liftNode { t: Any =>
          (f1(t), f2(t))
        })
      case (Right(f1), Right(f2)) =>
        Right({ t: Any =>
          (f1(t), f2(t))
        })
    }
    fx match {
      case Left(f) =>
        ValueInvocationElement(asNode { () =>
          aggregate(sf(), f, cor)
        })
      case Right(f) =>
        ValueInvocationElement(asNode { () =>
          aggregateSync(sf(), f, cor)
        })
    }
  }

  protected def rewriteAsIterableFunc(f: FuncElement, method: String): RelationElement = {
    val CollectionInvocationElement(sf) = visitElement(f.instance)
    method match {
      case "head" =>
        ValueInvocationElement(asNode { () =>
          sf().head
        })
      case "headOption" =>
        ValueInvocationElement(asNode { () =>
          sf().headOption
        })
      case "last" =>
        ValueInvocationElement(asNode { () =>
          sf().last
        })
      case "lastOption" =>
        ValueInvocationElement(asNode { () =>
          sf().lastOption
        })
      case _ => throw new RelationalException("Not implemented!")
    }
  }

  protected def rewriteSimpleCount(f: FuncElement): RelationElement = {
    val CollectionInvocationElement(sf) = visitElement(f.instance)
    ValueInvocationElement(asNode { () =>
      sf().size.toLong
    })
  }

  protected def rewriteAggregate(f: FuncElement): RelationElement = {
    val CollectionInvocationElement(sf) = visitElement(f.instance)
    if (f.arguments.size == 1) {
      val ConstValueElement(aggr: Aggregator[Any @unchecked], _) = f.arguments.head
      ValueInvocationElement(asNode { () =>
        aggregateSync(sf(), aggr)
      })
    } else {
      val FuncElement(c: ScalaLambdaCallee[_, _], _, _) :: ConstValueElement(aggr: Aggregator[Any @unchecked], _) :: _ =
        f.arguments
      c.lambda.asInstanceOf[NodeOrFunc[Any, Any]] match {
        case Left(nf) =>
          ValueInvocationElement(asNode { () =>
            aggregate(sf(), nf, aggr)
          })
        case Right(f) =>
          ValueInvocationElement(asNode { () =>
            aggregateSync(sf(), f, aggr)
          })
      }
    }
  }

  protected def rewriteUnion(m: MethodElement): RelationElement = {
    val isSyncIter = m.methodArgs.forall(_.param match {
      case i: IterableSource[_] => i.isSyncSafe
      case _                    => false
    })
    val invoke = if (isSyncIter) {
      val elemArgs = m.methodArgs.map(_.param.asInstanceOf[IterableSource[_]])
      CollectionInvocationElement(asNode { () =>
        elemArgs.flatMap(_.getSync())
      })
    } else {
      val nodeFuncList = m.methodArgs.map(arg => {
        val CollectionInvocationElement(sf) = visitElement(arg.param)
        sf
      })
      CollectionInvocationElement(asNode { () =>
        union(nodeFuncList)
      })
    }
    distinctByKey(invoke, m.key)
  }

  protected def rewriteMerge(m: MethodElement): RelationElement = {
    val src :: ConstValueElement(ag: Aggregator[Any @unchecked], _) :: others = m.methodArgs.map(_.param)
    val groupKey = ModuloKey(m.rowTypeInfo.cast[Any], ag.resultType.cast[Any])
    val isSyncKey = groupKey.isSyncSafe
    val isSyncIter = (src :: others).forall(_ match {
      case i: IterableSource[_] => i.isSyncSafe
      case _                    => false
    })
    if (isSyncIter) {
      val elemArgs = (src :: others).map(_.asInstanceOf[IterableSource[_]])
      CollectionInvocationElement(asNode { () =>
        if (isSyncKey) mergeSync(elemArgs.flatMap(_.getSync()), groupKey, m.rowTypeInfo, ag)
        else merge(elemArgs.flatMap(_.getSync()), groupKey, m.rowTypeInfo, ag)
      })
    } else {
      val nodeFuncList = (src :: others).map(arg => {
        val CollectionInvocationElement(sf) = visitElement(arg)
        sf
      })
      CollectionInvocationElement(asNode { () =>
        if (isSyncKey) mergeSync(union(nodeFuncList), groupKey, m.rowTypeInfo, ag)
        else merge(union(nodeFuncList), groupKey, m.rowTypeInfo, ag)
      })
    }
  }

  protected def rewriteTupleJoin(m: MethodElement): RelationElement = {
    import JoinHelper._

    val (MethodArg(_, left: MultiRelationElement) :: MethodArg(_, right: MultiRelationElement) :: others) = m.methodArgs
    val CollectionInvocationElement(sf1) = visitElement(left)
    val CollectionInvocationElement(sf2) = visitElement(right)
    val leftMultiKey = findFuncElement(others, "leftMultiKey")
    val rightMultiKey = findFuncElement(others, "rightMultiKey")
    val on = findFuncElement(others, "on")
    val leftDefaultFunc = getWithDefaultLambda(m.methodCode, others, Left(left.projectedType()))
    val rightDefaultFunc = getWithDefaultLambda(m.methodCode, others, Right(right.projectedType()))

    (leftMultiKey, rightMultiKey, on) match {
      case (Some(l), Some(r), _) =>
        val lf = FuncElement.convertFuncElementToLambda(l)
        val rf = FuncElement.convertFuncElementToLambda(r)
        CollectionInvocationElement(asNode { () =>
          val (leftDataMap, rightDataMap) = getDataMapFromMultiKeyLambda(sf1(), sf2(), lf, rf)
          joinOnSelectKey(leftDataMap, rightDataMap, m.methodCode, leftDefaultFunc, rightDefaultFunc)
        })
      case (_, _, Some(o)) =>
        val f = FuncElement.convertFuncElementToLambda2[Any, Any, Boolean](o)
        CollectionInvocationElement(asNode { () =>
          joinOnLambda(sf1(), sf2(), m.methodCode, f, leftDefaultFunc, rightDefaultFunc)
        })
      case _ =>
        val (leftKey, rightKey) = getTupleJoinKeys(left, right, m.methodCode)
        CollectionInvocationElement(asNode { () =>
          val (leftDataMap, rightDataMap) = getDataMapFromKeyFields(sf1(), sf2(), leftKey, rightKey)
          joinOnSelectKey(leftDataMap, rightDataMap, m.methodCode, leftDefaultFunc, rightDefaultFunc)
        })
    }
  }

  protected def rewriteNaturalJoin(m: MethodElement): RelationElement = {
    import JoinHelper._
    import QueryMethod._

    val (MethodArg(_, left: MultiRelationElement) :: MethodArg(_, right: MultiRelationElement) :: others) = m.methodArgs
    val CollectionInvocationElement(sf1) = visitElement(left)
    val CollectionInvocationElement(sf2) = visitElement(right)
    val leftType = left.projectedType()
    val rightType = right.projectedType()
    val leftDefaultFunc = getWithDefaultLambda(m.methodCode, others, Left(leftType))
    val rightDefaultFunc = getWithDefaultLambda(m.methodCode, others, Right(rightType))
    val sideAProxyFactory = ExtensionProxyFactory(
      leftType,
      rightType,
      false
    ) // generated right side will never overwrite left in full join and left outer join
    val sideAProxyLambda = (leftVal: AnyRef, rightVal: AnyRef) => sideAProxyFactory.proxy(leftVal, rightVal)
    val (leftKey, rightKey) = getNaturalJoinKeys(left, right, m.methodCode)

    m.methodCode match {
      case NATURAL_INNER_JOIN | NATURAL_LEFT_OUTER_JOIN =>
        CollectionInvocationElement(asNode { () =>
          val (leftDataMap, rightDataMap) = getDataMapFromKeyFields(sf1(), sf2(), leftKey, rightKey)
          joinOnSelectKeyNatural(
            leftDataMap,
            rightDataMap,
            m.methodCode,
            leftDefaultFunc,
            rightDefaultFunc,
            sideAProxyLambda,
            null)
        })
      case NATURAL_RIGHT_OUTER_JOIN | NATURAL_FULL_OUTER_JOIN => {
        val sideBProxyFactory = ExtensionProxyFactory(
          leftType,
          rightType,
          true
        ) // generated left side will never overwrite right in full join and right outer join
        val sideBProxyLambda = (leftVal: AnyRef, rightVal: AnyRef) => sideBProxyFactory.proxy(leftVal, rightVal)
        CollectionInvocationElement(asNode { () =>
          val (leftDataMap, rightDataMap) = getDataMapFromKeyFields(sf1(), sf2(), leftKey, rightKey)
          joinOnSelectKeyNatural(
            leftDataMap,
            rightDataMap,
            m.methodCode,
            leftDefaultFunc,
            rightDefaultFunc,
            sideAProxyLambda,
            sideBProxyLambda)
        })
      }
    }
  }

  protected def rewriteDifference(m: MethodElement): RelationElement = {
    val (src :: other :: _) = m.methodArgs
    val CollectionInvocationElement(srcSf) = visitElement(src.param)
    val CollectionInvocationElement(otherSf) = visitElement(other.param)
    CollectionInvocationElement(asNode { () =>
      difference(srcSf(), otherSf(), m.key.asInstanceOf[RelationKey[Any]])
    })
  }
}

object IterableRewriter {
  private val iterAny = {
    val t = TypeInfo.javaTypeInfo(classOf[Iterable[_]])
    t.copy(typeParams = Seq(TypeInfo.ANY))
  }

  def rewriteAndCompile[T](e: RelationElement): NodeFunction0[T] = {
    new IterableRewriter().rewriteAndCompile[T](e)
  }
}

private class IterableResultCache(nf: NodeFunction0[Iterable[Any]], refCount: Int) {
  import optimus.core._

  private val logger = scalalog.getLogger[IterableRewriter]
  private var cache: Node[Iterable[Any]] = null
  private var ref = 0

  @nodeSync
  def apply(): Iterable[Any] = needsPlugin
  def apply$queued(): Node[Iterable[Any]] = apply$newNode().enqueueAttached
  def apply$withNode(): Iterable[Any] = apply$newNode().get
  def apply$newNode(): Node[Iterable[Any]] = {
    val (c, r) = this.synchronized {
      if (cache eq null) {
        ref = refCount
        cache = new Cache()
        // This assumes that we will never call apply() under multiple scenario stacks, as
        // we're essentially choosing one randomly and freezing it in place.
        // If we did not attach here (and called .enqueue instead of .enqueueAttached above),
        // this ambiguity would still exist, and in addition there would be a chance of a
        // GraphException if we managed to get enqueued more than once before running.
        cache.attach(EvaluationContext.scenarioStack)
      }
      val ret = (cache, ref)
      ref -= 1
      if (ref <= 0) {
        cache = null
      }
      ret
    }
    logger.debug(s"IterableResultCache@${hashCode}: cached@${c.hashCode}; ref: $r")
    c
  }

  class Cache extends CompletableNode[Iterable[Any]] {
    override def run(ec: OGSchedulerContext): Unit = {
      nf.apply$queued().continueWith(this, ec)
    }
    override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      completeFromNode(child.asInstanceOf[Node[Iterable[Any]]], eq)
    }
  }
}
