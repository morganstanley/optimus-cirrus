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
package optimus.platform.relational

import optimus.platform._
import optimus.platform._
import optimus.platform.internal.MacroBase
import optimus.platform.relational.aggregation._
import optimus.platform.relational.tree.LambdaElement
import optimus.platform.relational.tree.MethodPosition
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.Entity
import optimus.platform.util.ReflectUtils

import scala.collection.mutable.ListBuffer
import scala.reflect.macros.blackbox.Context

private[relational] class QueryMacros[C <: Context](val c: C) extends MacroBase {
  import c.universe._
  import scala.util.Try

  /**
   * Creates a compile time error if T is a final class.
   */
  def assertNotFinal[T: c.WeakTypeTag]: Unit =
    if (c.symbolOf[T].isFinal)
      c.error(c.macroApplication.pos, s"priql macro creates illegal inheritance from final class ${c.symbolOf[T].name}")
  def assertNotFinal[T: c.WeakTypeTag, U: c.WeakTypeTag]: Unit = { assertNotFinal[T]; assertNotFinal[U] }

  def sortBy[T: c.WeakTypeTag, U: c.WeakTypeTag](f: c.Expr[T => U])(
      ordering: c.Expr[Ordering[U]],
      sortType: c.Expr[TypeInfo[U]],
      pos: c.Expr[MethodPosition]): c.Expr[Query[T]] = {
    val prefix = c.prefix.cast[Query[T]]
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    reify {
      QueryOps(prefix.splice).sortBy[U](lambda.splice)(ordering.splice, sortType.splice, pos.splice)
    }
  }

  def map[T: c.WeakTypeTag, U: c.WeakTypeTag](
      f: c.Expr[T => U])(resultType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[U]] = {
    if (isRedundantMap(f)) {
      val prefix = c.prefix.cast[Query[U]]
      prefix
    } else {
      val prefix = c.prefix.cast[Query[T]]
      val lamOptExpr = reifyAsLambdaElement(f)
      val lambda = mkLambda1(rewriteAnonClassDef(f), lamOptExpr)
      reify {
        QueryOps(prefix.splice).map[U](lambda.splice)(resultType.splice, pos.splice)
      }
    }
  }

  def flatMap[T: c.WeakTypeTag, U: c.WeakTypeTag, F[_]](f: c.Expr[T => F[U]])(
      conv: c.Expr[QueryConverter[U, F]],
      resultType: c.Expr[TypeInfo[U]],
      pos: c.Expr[MethodPosition])(implicit ftag: c.WeakTypeTag[F[_]]): c.Expr[Query[U]] = {
    val prefix = c.prefix.cast[Query[T]]
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(rewriteAnonClassDef(f), lamOptExpr)
    reify {
      QueryOps(prefix.splice).flatMap(lambda.splice)(conv.splice, resultType.splice, pos.splice)
    }
  }

  def filter[T: c.WeakTypeTag](p: c.Expr[T => Boolean])(pos: c.Expr[MethodPosition]): c.Expr[Query[T]] = {
    val prefix = c.prefix.cast[Query[T]]
    val lamOptExpr = reifyAsLambdaElement(p)
    val lambda = mkLambda1(p, lamOptExpr)
    reify {
      QueryOps(prefix.splice).filter(lambda.splice)(pos.splice)
    }
  }

  def groupBy[T: c.WeakTypeTag, U: c.WeakTypeTag](
      f: c.Expr[T => U])(keyType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[GroupQuery[U, Query[T]]] = {
    val prefix = c.prefix.cast[Query[T]]
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    reify {
      QueryOps(prefix.splice).groupBy[U](lambda.splice)(keyType.splice, pos.splice)
    }
  }

  def aggregateBy[
      T: c.WeakTypeTag,
      GroupKeyType: c.WeakTypeTag,
      GroupValueType: c.WeakTypeTag,
      AggregateType: c.WeakTypeTag](f: c.Expr[Query[GroupValueType] => AggregateType])(
      groupKeyType: c.Expr[TypeInfo[GroupKeyType]],
      groupValueType: c.Expr[TypeInfo[GroupValueType]],
      aggregateType: c.Expr[TypeInfo[AggregateType]],
      pos: c.Expr[MethodPosition]): c.Expr[Query[GroupKeyType with AggregateType]] = {
    assertNotFinal[AggregateType, GroupKeyType]
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).aggregateByExplicit[GroupKeyType, GroupValueType, AggregateType](lambda.splice)(
        groupKeyType.splice,
        groupValueType.splice,
        aggregateType.splice,
        pos.splice)
    }
  }

  def aggregateBy[
      T: c.WeakTypeTag,
      GroupKeyType: c.WeakTypeTag,
      GroupValueType: c.WeakTypeTag,
      AggregateType: c.WeakTypeTag](
      groupKeyType: c.Expr[TypeInfo[GroupKeyType]],
      groupValueType: c.Expr[TypeInfo[GroupValueType]],
      aggregateType: c.Expr[TypeInfo[AggregateType]],
      pos: c.Expr[MethodPosition]): c.Expr[Query[GroupKeyType with AggregateType]] = {
    assertNotFinal[AggregateType, GroupKeyType]
    val prefix = c.prefix.cast[Query[T]]

    val (impls, aggregateTypes) = MacrosHelper.getNodeFuncTreesAndAggregateTypes(c)(
      c.weakTypeTag[GroupValueType].tpe,
      c.weakTypeTag[AggregateType].tpe)
    val nodeFuncTrees =
      c.Expr[List[NodeFunction1[_, _]]](Apply(Select(Ident(typeOf[List.type].termSymbol), TermName("apply")), impls))
    val aggregateTypesTree =
      c.Expr[List[TypeInfo[_]]](Apply(Select(Ident(typeOf[List.type].termSymbol), TermName("apply")), aggregateTypes))
    reify {
      val aggregatorNodeFuncs = nodeFuncTrees.splice
      val aggregateTypes = aggregateTypesTree.splice
      QueryOps(prefix.splice).aggregateByImplicit[GroupKeyType, GroupValueType, AggregateType](
        aggregatorNodeFuncs,
        aggregateTypes)(groupKeyType.splice, groupValueType.splice, aggregateType.splice, pos.splice)
    }
  }

  def customAggregate[T: c.WeakTypeTag](f: c.Expr[Query[T] => AnyRef]): c.Expr[NodeFunction1[Query[T], T]] = {
    c.Expr[NodeFunction1[Query[T], T]](MacrosHelper.constructNodeFuncTree(c)(c.weakTypeTag[T].tpe, f.tree))
  }

  def mapValues[T: c.WeakTypeTag, U: c.WeakTypeTag, S: c.WeakTypeTag](
      f: c.Expr[U => S])(resultType: c.Expr[TypeInfo[S]], pos: c.Expr[MethodPosition]): c.Expr[GroupQuery[T, S]] = {
    val prefix = c.prefix.cast[GroupQuery[T, U]]
    val lamOptExpr = reifyAsLambdaElement(f)

    val lambda = mkLambda1(rewriteAnonClassDef(f), lamOptExpr)
    val t = reify {
      GroupQueryOps(prefix.splice).mapValues[S](lambda.splice)(resultType.splice, pos.splice)
    }
    t
  }

  def extendTyped[T: c.WeakTypeTag, U: c.WeakTypeTag](
      f: c.Expr[T => U])(extensionType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[T with U]] = {
    assertNotFinal[T, U]
    val lambda = mkLambda1(f)
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).extendTyped[U](lambda.splice)(extensionType.splice, pos.splice)
    }
  }

  def extendTypedValue[T: c.WeakTypeTag, U: c.WeakTypeTag](
      value: c.Expr[U])(extensionType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[T with U]] = {
    assertNotFinal[T, U]
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).extendTypedValue[U](value.splice)(extensionType.splice, pos.splice)
    }
  }

  def replace[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](
      f: c.Expr[T => U])(replaceType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[T]] = {
    assertNotFinal[T]
    val lambda = mkLambda1(f)
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).replace[U](lambda.splice)(replaceType.splice, pos.splice)
    }
  }

  def replaceValue[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](
      repVal: c.Expr[U])(replaceType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[T]] = {
    assertNotFinal[T]
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).replaceValue[U](repVal.splice)(replaceType.splice, pos.splice)
    }
  }

  def extend[T: c.WeakTypeTag, U: c.WeakTypeTag](field: c.Expr[String], f: c.Expr[DynamicObject => U])(
      fieldType: c.Expr[TypeInfo[U]],
      pos: c.Expr[MethodPosition]): c.Expr[Query[DynamicObject]] = {
    val lambda = mkLambda1(f)
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).extend[U](field.splice, lambda.splice)(fieldType.splice, pos.splice)
    }
  }

  def shapeTo[T: c.WeakTypeTag, U: c.WeakTypeTag](f: c.Expr[DynamicObject => U])(
      shapeToType: c.Expr[TypeInfo[U]],
      pos: c.Expr[MethodPosition]): c.Expr[Query[U]] = {
    val lambda = mkLambda1(rewriteAnonClassDef(f))
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).shapeTo[U](lambda.splice)(shapeToType.splice, pos.splice)
    }
  }

  def withLeftDefault[B: c.WeakTypeTag, R: c.WeakTypeTag](
      f: c.Expr[R => B])(b: c.Expr[TypeInfo[B]], pos: c.Expr[MethodPosition]): c.Expr[JoinQuery[B, R]] = {
    val prefix = c.prefix.cast[JoinQuery[B, R]]
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    reify {
      JoinQueryOps(prefix.splice)
        .withLeftDefault[B](lambda.splice)(b.splice, pos.splice)
    }
  }

  def withRightDefault[L: c.WeakTypeTag, B: c.WeakTypeTag](
      f: c.Expr[L => B])(b: c.Expr[TypeInfo[B]], pos: c.Expr[MethodPosition]): c.Expr[JoinQuery[L, B]] = {
    val prefix = c.prefix.cast[JoinQuery[L, B]]
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    reify {
      JoinQueryOps(prefix.splice)
        .withRightDefault[B](lambda.splice)(b.splice, pos.splice)
    }
  }

  def withLeftDefaultNatural[B: c.WeakTypeTag, R: c.WeakTypeTag](
      f: c.Expr[R => B])(b: c.Expr[TypeInfo[B]], pos: c.Expr[MethodPosition]): c.Expr[NaturalJoinQuery[B, R]] = {
    assertNotFinal[B, R]
    val prefix = c.prefix.cast[NaturalJoinQuery[B, R]]
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    reify {
      NaturalJoinQueryOps(prefix.splice)
        .withLeftDefaultNatural[B](lambda.splice)(b.splice, pos.splice)
    }
  }

  def withRightDefaultNatural[L: c.WeakTypeTag, B: c.WeakTypeTag](
      f: c.Expr[L => B])(b: c.Expr[TypeInfo[B]], pos: c.Expr[MethodPosition]): c.Expr[NaturalJoinQuery[L, B]] = {
    assertNotFinal[B, L]
    val prefix = c.prefix.cast[NaturalJoinQuery[L, B]]
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    reify {
      NaturalJoinQueryOps(prefix.splice)
        .withRightDefaultNatural[B](lambda.splice)(b.splice, pos.splice)
    }
  }

  def on[L: c.WeakTypeTag, R: c.WeakTypeTag](f: c.Expr[(L, R) => Boolean]): c.Expr[JoinQuery[L, R]] = {
    val prefix = c.prefix.cast[JoinQuery[L, R]]
    val lamOptExpr = reifyAsLambdaElement(f)
    val Some(Function(params @ List(par1, par2), body)) = f.tree.find(_ match {
      case func @ Function(params, body) if params.size == 2 => true
      case _                                                 => false
    })
    val literalTrue = Literal(Constant(true))
    val conditions = AndAlsoNormalizer.normalize(body).map { c =>
      ValDefRefChecker.check(c, params) match {
        // the condition contains both par1 and par2
        case List(_, _) =>
          EqualsNormalizer.normalize(c) match {
            // it is a equal condition
            case List(left, right) =>
              val leftRefs = ValDefRefChecker.check(left, params)
              val rightRefs = ValDefRefChecker.check(right, params)
              if (leftRefs.size == 1 && rightRefs.size == 1)
                if (leftRefs.head.symbol == par1.symbol) Option(left, right) else Option(right, left)
              else None
            case _ => None
          }

        // the condition contains only par1
        case List(valDef) if valDef.symbol == par1.symbol => Option(c, literalTrue)

        // the condition contains only par2 or contains neither par1 nor par2
        case _ => Option(literalTrue, c)
      }
    }

    if (conditions.forall(_.isDefined)) {

      def mkFunc[T: WeakTypeTag](valDef: ValDef, arrayItems: List[Tree]): c.Expr[T => MultiKey] = {
        import definitions._
        // Array[Any](arrayParams:_*)
        val array = Apply(TypeApply(Select(Ident(ArrayModule), TermName("apply")), List(Ident(AnyClass))), arrayItems)
        // new MultiKey(arr)
        val body = Apply(Select(New(Ident(typeOf[MultiKey].typeSymbol)), termNames.CONSTRUCTOR), List(array))
        c.Expr[T => MultiKey] {
          c.untypecheck(Function(List(valDef), body))
        }
      }

      conditions.unzip(_.get) match {
        case (left, right) =>
          val leftF = mkFunc[L](par1, left)
          val lambda1 = mkLambda1(leftF)

          val rightF = mkFunc[R](par2, right)
          val lambda2 = mkLambda1(rightF)

          val lambda = mkLambda2(null, lamOptExpr)
          reify {
            JoinQueryOps(prefix.splice).on(lambda.splice, Some(lambda1.splice), Some(lambda2.splice))
          }
      }
    } else {
      val lambda = mkLambda2(f, lamOptExpr)
      reify {
        JoinQueryOps(prefix.splice).on(lambda.splice, None, None)
      }
    }
  }

  def onNatural[L: c.WeakTypeTag, R: c.WeakTypeTag]: c.Expr[NaturalJoinQuery[L, R]] = {
    assertNotFinal[L, R]
    val prefix = c.prefix.cast[JoinQuery[L, R]]
    reify {
      JoinQueryOps(prefix.splice).onNatural
    }
  }

  def foldLeft[T: c.WeakTypeTag, U: c.WeakTypeTag](z: c.Expr[U])(op: c.Expr[(U, T) => U]): c.Expr[U] = {
    val lambda = mkLambda2(op, c.Expr(mkNone))
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).foldLeft(z.splice)(lambda.splice)
    }
  }

  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](op: c.Expr[(U, U) => U]): c.Expr[U] = {
    val lambda = mkLambda2(op, c.Expr(mkNone))
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).reduce(lambda.splice)
    }
  }

  // Wrapper around asInstanceOf to work around the horrors of reification + path-dependence
  // Consider aggregate1: we want to produce `QueryOps(prefix).aggregate[U](aggregator)`, which in normal Scala would
  // have the type `aggregator.Result`. However, `splice` is a def, and therefore doesn't form part of a stable path,
  // so `reify(QueryOps(prefix.splice).aggregate[U](aggregator.splice))` has widened type `Aggregator[U]#Result`, which
  // is no good. We do have `aggregator.value.Result` as a stable path, but it can't be referred to within the `reify`
  // block, since it then becomes a free type and reification fails. Therefore, this.
  // The below cast is guaranteed to be safe as long as `agg` is the same `Aggregator` that was used inside the `reify`.
  private[this] def fixAggregateType[U](agg: c.Expr[Aggregator[U]])(
      expr: c.Expr[Aggregator[U]#Result]): c.Expr[agg.value.Result] =
    expr.cast[agg.value.Result]

  def aggregate1[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](
      aggregator: c.Expr[Aggregator[U]]): c.Expr[aggregator.value.Result] = {
    val prefix = c.prefix.cast[Query[T]]
    fixAggregateType(aggregator)(reify {
      QueryOps(prefix.splice).aggregate[U](aggregator.splice)
    })
  }

  def aggregate2[T: c.WeakTypeTag, U: c.WeakTypeTag](f: c.Expr[T => U], aggregator: c.Expr[Aggregator[U]])(
      aggregateType: c.Expr[TypeInfo[U]]): c.Expr[aggregator.value.Result] = {
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    val prefix = c.prefix.cast[Query[T]]
    fixAggregateType(aggregator)(reify {
      QueryOps(prefix.splice).aggregate[U](lambda.splice, aggregator.splice)(aggregateType.splice)
    })
  }

  def sum[T: c.WeakTypeTag, U: c.WeakTypeTag](f: c.Expr[T => U])(sum: c.Expr[Sum[U]]): c.Expr[U] = {
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).sum[U](lambda.splice)(sum.splice)
    }
  }

  def average1[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](avg: c.Expr[Avg[U]]): c.Expr[avg.value.Result] = {
    val prefix = c.prefix.cast[Query[T]]
    fixAggregateType(avg)(reify {
      QueryOps(prefix.splice).average[U](avg.splice)
    })
  }

  def average2[T: c.WeakTypeTag, U: c.WeakTypeTag](f: c.Expr[T => U])(avg: c.Expr[Avg[U]]): c.Expr[avg.value.Result] = {
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    val prefix = c.prefix.cast[Query[T]]
    fixAggregateType(avg)(reify {
      QueryOps(prefix.splice).average[U](lambda.splice)(avg.splice)
    })
  }

  def variance1[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](v: c.Expr[Variance[U]]): c.Expr[v.value.Result] = {
    val prefix = c.prefix.cast[Query[T]]
    fixAggregateType(v)(reify {
      QueryOps(prefix.splice).variance[U](v.splice)
    })
  }

  def variance2[T: c.WeakTypeTag, U: c.WeakTypeTag](f: c.Expr[T => U])(
      v: c.Expr[Variance[U]]): c.Expr[v.value.Result] = {
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    val prefix = c.prefix.cast[Query[T]]
    fixAggregateType(v)(reify {
      QueryOps(prefix.splice).variance[U](lambda.splice)(v.splice)
    })
  }

  def stddev1[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](
      stdDev: c.Expr[StandardDeviation[U]]): c.Expr[stdDev.value.Result] = {
    val prefix = c.prefix.cast[Query[T]]
    fixAggregateType(stdDev)(reify {
      QueryOps(prefix.splice).stddev[U](stdDev.splice)
    })
  }

  def stddev2[T: c.WeakTypeTag, U: c.WeakTypeTag](f: c.Expr[T => U])(
      stdDev: c.Expr[StandardDeviation[U]]): c.Expr[stdDev.value.Result] = {
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    val prefix = c.prefix.cast[Query[T]]
    fixAggregateType(stdDev)(reify {
      QueryOps(prefix.splice).stddev[U](lambda.splice)(stdDev.splice)
    })
  }

  def corr[T: c.WeakTypeTag](x: c.Expr[T => Double], y: c.Expr[T => Double])(
      cor: c.Expr[Correlation[Double]]): c.Expr[Double] = {
    val lamOptExpr1 = reifyAsLambdaElement(x)
    val lambda1 = mkLambda1(x, lamOptExpr1)
    val lamOptExpr2 = reifyAsLambdaElement(y)
    val lambda2 = mkLambda1(y, lamOptExpr2)
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).corr(lambda1.splice, lambda2.splice)(cor.splice)
    }
  }

  def min[T: c.WeakTypeTag, U: c.WeakTypeTag](f: c.Expr[T => U])(min: c.Expr[Min[U]]): c.Expr[U] = {
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).min[U](lambda.splice)(min.splice)
    }
  }

  def max[T: c.WeakTypeTag, U: c.WeakTypeTag](f: c.Expr[T => U])(max: c.Expr[Max[U]]): c.Expr[U] = {
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).max[U](lambda.splice)(max.splice)
    }
  }

  def count[T: c.WeakTypeTag](f: c.Expr[T => Boolean]): c.Expr[Long] = {
    val lamOptExpr = reifyAsLambdaElement(f)
    val lambda = mkLambda1(f, lamOptExpr)
    val prefix = c.prefix.cast[Query[T]]
    reify {
      QueryOps(prefix.splice).count(lambda.splice)
    }
  }

  private def isRedundantMap[T](f: c.Expr[T]): Boolean = {
    f.tree match {
      case Function(List(valDef: ValDef), ident: Ident) => valDef.symbol == ident.symbol
      case _                                            => false
    }
  }

  def rewriteAnonClassDef[T](f: c.Expr[T]): c.Expr[T] = {
    import c.internal._

    val t = transform(f.tree)((tree, api) =>
      tree match {
        case Block(
              List(cls @ ClassDef(mods, name, tparams, impl @ Template(parents, self, body))),
              nex @ Apply(Select(New(Ident(_)), _), Nil)) =>
          val anyRefSymbol = definitions.AnyRefTpe.typeSymbol
          val (tpes, sMethods) = ReflectUtils.extractRealTypesAndStructuralMethods(c.universe)(tree.tpe)
          // check if tpe inherits Entity or the 'equals' is overridden
          if (
            tpes.exists(t => t <:< typeOf[Entity]) ||
            sMethods.exists(m => m.name == TermName("equals") && m.overrides.exists(_.owner == anyRefSymbol))
          ) {
            api.default(tree)
          } else {
            val filteredTpes = tpes.filterNot(t => t =:= definitions.AnyTpe || t.typeSymbol == definitions.ObjectClass)
            val productSymbol = typeOf[Product].typeSymbol
            val nsProps = filteredTpes
              .flatMap(t => t.members)
              .filter(m => m.isMethod && m.isPublic && !m.isStatic)
              .filter(m => !(m :: m.overrides).exists(o => o.owner == anyRefSymbol || o.owner == productSymbol))
              .filter { m =>
                val msym = m.asMethod
                msym.paramLists match {
                  case Nil | List(Nil) =>
                    msym.typeParams.isEmpty &&
                    !(msym.returnType =:= definitions.UnitTpe) &&
                    !m.name.decodedName.toString().contains("$")
                  case _ => false
                }
              }
            val sProps = sMethods.filter(m =>
              m.overrides.isEmpty && m.paramLists.isEmpty && !(m.returnType =:= definitions.UnitTpe))
            val props = nsProps ++ sProps

            val anonSym = cls.symbol
            val anonThis = gen.mkAttributedThis(anonSym)

            // hashCode
            val hashCodeSym = newMethodSymbol(anonSym, TermName("hashCode"), anonSym.pos.focus, Flag.OVERRIDE)
            setInfo(hashCodeSym, nullaryMethodType(definitions.IntTpe))
            enter(anonSym.info.decls, hashCodeSym)
            val hashCalc = q"var acc = 37" +: props.map { p =>
              q"acc = acc * 17 + $anonThis.${p.name.toTermName}.##"
            }
            val hashCodeDef =
              typingTransform(defDef(hashCodeSym, q"..$hashCalc; acc"), anonSym)((t, api) => api.typecheck(t))

            // equals
            val equalsSym = newMethodSymbol(anonSym, TermName("equals"), anonSym.pos.focus, Flag.OVERRIDE)
            val otherSym = newTermSymbol(equalsSym, TermName("other"), equalsSym.pos.focus, Flag.PARAM)
            setInfo(otherSym, definitions.AnyTpe)
            setInfo(equalsSym, methodType(List(otherSym), definitions.BooleanTpe))
            enter(anonSym.info.decls, equalsSym)
            val equalsParts = props.map { p =>
              q"$anonThis.${p.name.toTermName} == o.${p.name.toTermName}"
            }
            val interfaceCompare = q"$anonThis.getClass == o.getClass"
            val equalsCalc = equalsParts.foldLeft(interfaceCompare)((z, t) => q"$z && $t")
            val equalsRhs =
              q"${otherSym.name} match { case o: ${TypeTree(tree.tpe)} => if (o eq $anonThis) true else $equalsCalc; case _ => false }"
            val equalsDef = typingTransform(defDef(equalsSym, equalsRhs), anonSym)((t, api) => api.typecheck(t))

            val newImpl = treeCopy.Template(impl, parents, self, hashCodeDef :: equalsDef :: body)
            val newClassDef = treeCopy.ClassDef(cls, mods, name, tparams, newImpl)
            treeCopy.Block(tree, List(newClassDef), nex)
          }
        case _ => api.default(tree)
      })
    c.Expr[T](t)
  }

  def reifyAsLambdaElement[T](f: c.Expr[T]): c.Expr[Option[() => Try[LambdaElement]]] = {
    val lamExpr = new LambdaReifier[c.type](c).reifyOption(f.tree)
    lamExpr.map { t =>
      reify { Some(() => Try(PostMacroRewriter.rewrite(t.splice))) }
    } getOrElse (c.Expr(mkNone))
  }

  def reifyLambda[T](f: c.Expr[T]): c.Expr[Option[LambdaElement]] = {
    val lamExpr = new LambdaReifier[c.type](c).reifyOption(f.tree)
    lamExpr.map { t =>
      reify { Try(t.splice).toOption }
    } getOrElse (c.Expr(mkNone))
  }

  private def mkLambda1[T, U](
      f: c.Expr[T => U],
      reifiedLambda: c.Expr[Option[() => Try[LambdaElement]]]): c.Expr[Lambda1[T, U]] = {
    reify { Lambda1.create(f.splice, reifiedLambda.splice) }
  }

  private def mkLambda1[T, U](f: c.Expr[T => U]): c.Expr[Lambda1[T, U]] = {
    mkLambda1[T, U](f, c.Expr(mkNone))
  }

  private def mkLambda2[T, U, S](
      f: c.Expr[(T, U) => S],
      reifiedLambda: c.Expr[Option[() => Try[LambdaElement]]]): c.Expr[Lambda2[T, U, S]] = {
    if (f eq null) {
      reify { Lambda2(None, None, reifiedLambda.splice) }
    } else {
      reify { Lambda2.create(f.splice, reifiedLambda.splice) }
    }
  }

  implicit class ExprOps(val origin: c.Expr[_]) {
    def cast[T] = origin.asInstanceOf[c.Expr[T]]
  }

  object ValDefRefChecker {
    def check(tree: Tree, params: List[ValDef]) = new ValDefRefChecker(params).check(tree)
  }

  class ValDefRefChecker(params: List[ValDef]) extends Traverser {
    private[this] val list = new ListBuffer[ValDef]()

    def check(tree: Tree): List[ValDef] = {
      list.clear()
      traverse(tree)
      list.distinct.result()
    }

    override def traverse(tree: Tree): Unit = tree match {
      case i @ Ident(_) => params.find(p => p.symbol == i.symbol).foreach(list += _)
      case _            => super.traverse(tree)
    }
  }

  class BinaryApplyNormalizer(op: Set[String], stepInto: Boolean = true) extends Traverser {
    private[this] val list = new ListBuffer[Tree]()

    def normalize(tree: Tree): List[Tree] = {
      traverse(tree)
      list.result()
    }

    override def traverse(tree: Tree): Unit = tree match {
      case Block(List(), expr) => traverse(expr)
      case Apply(Select(left, name), List(right)) if op.contains(name.decodedName.toString) =>
        if (stepInto) {
          traverse(left)
          traverse(right)
        } else {
          list += left
          list += right
        }
      case _ => list += tree
    }
  }

  object AndAlsoNormalizer {
    def normalize(tree: Tree) = new BinaryApplyNormalizer(Set("&&")).normalize(tree)
  }

  object EqualsNormalizer {
    def normalize(tree: Tree) = new BinaryApplyNormalizer(Set("==", "==="), false).normalize(tree)
  }

  private def mkNone = Ident(c.weakTypeOf[None.type].termSymbol)
}

object QueryMacros {
  def map[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(
      f: c.Expr[T => U])(resultType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[U]] =
    new QueryMacros[c.type](c).map[T, U](f)(resultType, pos)
  def flatMap[T: c.WeakTypeTag, U: c.WeakTypeTag, F[_]](c: Context)(f: c.Expr[T => F[U]])(
      conv: c.Expr[QueryConverter[U, F]],
      resultType: c.Expr[TypeInfo[U]],
      pos: c.Expr[MethodPosition])(implicit ftag: c.WeakTypeTag[F[_]]): c.Expr[Query[U]] =
    new QueryMacros[c.type](c).flatMap[T, U, F](f)(conv, resultType, pos)
  def filter[T: c.WeakTypeTag](c: Context)(p: c.Expr[T => Boolean])(pos: c.Expr[MethodPosition]): c.Expr[Query[T]] =
    new QueryMacros[c.type](c).filter[T](p)(pos)

  def aggregateByExplicit[
      T: c.WeakTypeTag,
      GroupKeyType: c.WeakTypeTag,
      GroupValueType: c.WeakTypeTag,
      AggregateType: c.WeakTypeTag](c: Context)(f: c.Expr[Query[GroupValueType] => AggregateType])(
      groupKeyType: c.Expr[TypeInfo[GroupKeyType]],
      groupValueType: c.Expr[TypeInfo[GroupValueType]],
      aggregateType: c.Expr[TypeInfo[AggregateType]],
      pos: c.Expr[MethodPosition]) =
    new QueryMacros[c.type](c)
      .aggregateBy[T, GroupKeyType, GroupValueType, AggregateType](f)(groupKeyType, groupValueType, aggregateType, pos)

  def aggregateByImplicit[
      T: c.WeakTypeTag,
      GroupKeyType: c.WeakTypeTag,
      GroupValueType: c.WeakTypeTag,
      AggregateType: c.WeakTypeTag](c: Context)(
      groupKeyType: c.Expr[TypeInfo[GroupKeyType]],
      groupValueType: c.Expr[TypeInfo[GroupValueType]],
      aggregateType: c.Expr[TypeInfo[AggregateType]],
      pos: c.Expr[MethodPosition]) =
    new QueryMacros[c.type](c)
      .aggregateBy[T, GroupKeyType, GroupValueType, AggregateType](groupKeyType, groupValueType, aggregateType, pos)

  def groupBy[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(
      f: c.Expr[T => U])(keyType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[GroupQuery[U, Query[T]]] =
    new QueryMacros[c.type](c).groupBy[T, U](f)(keyType, pos)
  def mapValues[T: c.WeakTypeTag, U: c.WeakTypeTag, S: c.WeakTypeTag](c: Context)(
      f: c.Expr[U => S])(resultType: c.Expr[TypeInfo[S]], pos: c.Expr[MethodPosition]): c.Expr[GroupQuery[T, S]] =
    new QueryMacros[c.type](c).mapValues[T, U, S](f)(resultType, pos)

  def extendTyped[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(
      f: c.Expr[T => U])(extensionType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[T with U]] =
    new QueryMacros[c.type](c).extendTyped[T, U](f)(extensionType, pos)

  def extendTypedValue[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(
      extVal: c.Expr[U])(extensionType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[T with U]] =
    new QueryMacros[c.type](c).extendTypedValue[T, U](extVal)(extensionType, pos)

  def replace[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(
      f: c.Expr[T => U])(replaceType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[T]] =
    new QueryMacros[c.type](c).replace[T, U](f)(replaceType, pos)

  def replaceValue[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(
      repVal: c.Expr[U])(replaceType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[T]] =
    new QueryMacros[c.type](c).replaceValue[T, U](repVal)(replaceType, pos)

  def on[L: c.WeakTypeTag, R: c.WeakTypeTag](c: Context)(f: c.Expr[(L, R) => Boolean]): c.Expr[JoinQuery[L, R]] =
    new QueryMacros[c.type](c).on[L, R](f)

  def onNatural[L: c.WeakTypeTag, R: c.WeakTypeTag](c: Context): c.Expr[NaturalJoinQuery[L, R]] =
    new QueryMacros[c.type](c).onNatural[L, R]

  def withLeftDefault[B: c.WeakTypeTag, R: c.WeakTypeTag](c: Context)(
      f: c.Expr[R => B])(b: c.Expr[TypeInfo[B]], pos: c.Expr[MethodPosition]): c.Expr[JoinQuery[B, R]] =
    new QueryMacros[c.type](c).withLeftDefault[B, R](f)(b, pos)
  def withRightDefault[L: c.WeakTypeTag, B: c.WeakTypeTag](c: Context)(
      f: c.Expr[L => B])(b: c.Expr[TypeInfo[B]], pos: c.Expr[MethodPosition]): c.Expr[JoinQuery[L, B]] =
    new QueryMacros[c.type](c).withRightDefault[L, B](f)(b, pos)

  def withLeftDefaultNatural[B: c.WeakTypeTag, R: c.WeakTypeTag](c: Context)(
      f: c.Expr[R => B])(b: c.Expr[TypeInfo[B]], pos: c.Expr[MethodPosition]): c.Expr[NaturalJoinQuery[B, R]] =
    new QueryMacros[c.type](c).withLeftDefaultNatural[B, R](f)(b, pos)
  def withRightDefaultNatural[L: c.WeakTypeTag, B: c.WeakTypeTag](c: Context)(
      f: c.Expr[L => B])(b: c.Expr[TypeInfo[B]], pos: c.Expr[MethodPosition]): c.Expr[NaturalJoinQuery[L, B]] =
    new QueryMacros[c.type](c).withRightDefaultNatural[L, B](f)(b, pos)

  def extend[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(field: c.Expr[String], f: c.Expr[DynamicObject => U])(
      fieldType: c.Expr[TypeInfo[U]],
      pos: c.Expr[MethodPosition]): c.Expr[Query[DynamicObject]] =
    new QueryMacros[c.type](c).extend[T, U](field, f)(fieldType, pos)
  def shapeTo[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(
      f: c.Expr[DynamicObject => U])(shapeToType: c.Expr[TypeInfo[U]], pos: c.Expr[MethodPosition]): c.Expr[Query[U]] =
    new QueryMacros[c.type](c).shapeTo[T, U](f)(shapeToType, pos)

  def sortBy[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U])(
      ordering: c.Expr[Ordering[U]],
      sortType: c.Expr[TypeInfo[U]],
      pos: c.Expr[MethodPosition]): c.Expr[Query[T]] =
    new QueryMacros[c.type](c).sortBy[T, U](f)(ordering, sortType, pos)

  def foldLeft[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(z: c.Expr[U])(op: c.Expr[(U, T) => U]): c.Expr[U] =
    new QueryMacros[c.type](c).foldLeft[T, U](z)(op)
  def reduce[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(op: c.Expr[(U, U) => U]): c.Expr[U] =
    new QueryMacros[c.type](c).reduce[T, U](op)
  def aggregate1[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(
      aggregator: c.Expr[Aggregator[U]]): c.Expr[aggregator.value.Result] =
    new QueryMacros[c.type](c).aggregate1[T, U](aggregator)
  def aggregate2[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U])(aggregator: c.Expr[Aggregator[U]])(
      aggregateType: c.Expr[TypeInfo[U]]): c.Expr[aggregator.value.Result] =
    new QueryMacros[c.type](c).aggregate2[T, U](f, aggregator)(aggregateType)
  def sum[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U])(sum: c.Expr[Sum[U]]): c.Expr[U] =
    new QueryMacros[c.type](c).sum[T, U](f)(sum)
  def average1[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(avg: c.Expr[Avg[U]]): c.Expr[avg.value.Result] =
    new QueryMacros[c.type](c).average1[T, U](avg)
  def average2[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U])(
      avg: c.Expr[Avg[U]]): c.Expr[avg.value.Result] =
    new QueryMacros[c.type](c).average2[T, U](f)(avg)
  def variance1[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(v: c.Expr[Variance[U]]): c.Expr[v.value.Result] =
    new QueryMacros[c.type](c).variance1[T, U](v)
  def variance2[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U])(
      v: c.Expr[Variance[U]]): c.Expr[v.value.Result] =
    new QueryMacros[c.type](c).variance2[T, U](f)(v)
  def stddev1[T: c.WeakTypeTag, U >: T: c.WeakTypeTag](c: Context)(
      stdDev: c.Expr[StandardDeviation[U]]): c.Expr[stdDev.value.Result] =
    new QueryMacros[c.type](c).stddev1[T, U](stdDev)
  def stddev2[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U])(
      stdDev: c.Expr[StandardDeviation[U]]): c.Expr[stdDev.value.Result] =
    new QueryMacros[c.type](c).stddev2[T, U](f)(stdDev)
  def corr[T: c.WeakTypeTag](c: Context)(x: c.Expr[T => Double], y: c.Expr[T => Double])(
      cor: c.Expr[Correlation[Double]]): c.Expr[Double] = new QueryMacros[c.type](c).corr[T](x, y)(cor)
  def min[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U])(min: c.Expr[Min[U]]): c.Expr[U] =
    new QueryMacros[c.type](c).min[T, U](f)(min)
  def max[T: c.WeakTypeTag, U: c.WeakTypeTag](c: Context)(f: c.Expr[T => U])(max: c.Expr[Max[U]]): c.Expr[U] =
    new QueryMacros[c.type](c).max[T, U](f)(max)
  def count[T: c.WeakTypeTag](c: Context)(f: c.Expr[T => Boolean]): c.Expr[Long] =
    new QueryMacros[c.type](c).count[T](f)
  def customAggregate[T: c.WeakTypeTag](c: Context)(f: c.Expr[Query[T] => AnyRef]): c.Expr[NodeFunction1[Query[T], T]] =
    new QueryMacros[c.type](c).customAggregate[T](f)
  def reifyLambda[T: c.WeakTypeTag](c: Context)(f: c.Expr[T]): c.Expr[Option[LambdaElement]] =
    new QueryMacros[c.type](c).reifyLambda[T](f)
}
