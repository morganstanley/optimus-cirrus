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
package optimus.platform.relational.tree

import optimus.core.needsPlugin
import optimus.core.utils.RuntimeMirror
import optimus.graph.AlreadyCompletedNode
import optimus.graph.Node
import optimus.graph.NodeFuture
import optimus.platform.AsyncCollectionHelpers
import optimus.platform.DynamicObject
import optimus.platform.annotations.dimension
import optimus.platform.annotations.nodeSync
import optimus.platform.annotations.parallelizable
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityImpl
import optimus.platform.util.ReflectUtils
import optimus.scalacompat.collection._
import org.objectweb.asm.{Type => AsmType}
import org.springframework.core.annotation.AnnotationUtils

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Modifier
import java.lang.reflect.{ParameterizedType => JavaParameterizedType}
import java.lang.reflect.{Type => JavaReflectType}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.macros.blackbox
import scala.reflect.macros.whitebox
import scala.reflect.runtime.universe._
import scala.runtime.BoxedUnit
import scala.runtime.Null$
import scala.runtime.ScalaRunTime

object TypeInfoUtils {

  final case class ParameterizedType(rawType: JavaReflectType, typeArgs: Seq[JavaReflectType])
      extends JavaParameterizedType {
    val getActualTypeArguments: Array[JavaReflectType] = typeArgs.toArray
    val getRawType = rawType
    def getOwnerType(): JavaReflectType = null
    override def toString =
      "" + getRawType + (if (getActualTypeArguments.isEmpty) "" else getActualTypeArguments.mkString("[", ", ", "]"))
  }

  def findAllPropertyMethod(c: Class[_], ignoreProxyFinal: Boolean): Map[String, Method] = {
    c.getMethods().iterator.collect { case m if isScalaProperty(m, ignoreProxyFinal) => m.getName -> m }.toMap
  }

  def extractJavaType(
      tpe: Type,
      mirror: Mirror,
      knownTypes: Map[Symbol, JavaReflectType] = Map.empty
  ): JavaReflectType = {
    if (tpe =:= typeOf[Any]) classOf[Object]
    else
      knownTypes.get(tpe.typeSymbol).getOrElse {
        tpe.dealias match {
          case dealiased @ TypeRef(_, _, realArgTypes) =>
            val javaCls =
              try {
                mirror.runtimeClass(dealiased)
              } catch {
                case e: NoClassDefFoundError => classOf[Object]
                case e: Exception            => throw e
              }
            realArgTypes match {
              case Nil => javaCls
              case args =>
                val argsArray: Array[JavaReflectType] =
                  args.iterator.map(a => extractJavaType(a, mirror, knownTypes)).toArray
                TypeInfoParameterizedType(argsArray.toList, javaCls)
            }
          case TypeBounds(lo, hi) => extractJavaType(hi, mirror, knownTypes)
          case ExistentialType(quantified, underlying) =>
            val quantifiedJavaTypes: Map[Symbol, JavaReflectType] =
              quantified.iterator.map(sym => sym -> extractJavaType(sym.typeSignature, mirror, knownTypes)).toMap
            extractJavaType(underlying, mirror, quantifiedJavaTypes ++ knownTypes)
          case t @ SingleType(_, symbol) => extractJavaType(symbol.typeSignature, mirror, knownTypes)
          case t @ PolyType(_, resType)  => extractJavaType(resType, mirror, knownTypes)
          case t @ RefinedType(parents, _) =>
            val pTypes = parents.map(p => extractJavaType(p, mirror, knownTypes))
            TypeInfoRefinedType(pTypes)
          case t @ ThisType(sym) =>
            val stpe = if (sym.isClass) sym.asClass.selfType else sym.typeSignature
            extractJavaType(stpe, mirror, knownTypes)
        }
      }
  }

  private[optimus] def isScalaProperty(m: Method, ignoreProxyFinal: Boolean): Boolean = {
    (m.getParameterTypes.isEmpty && !(ignoreProxyFinal && m.getDeclaredAnnotations.exists(
      _.isInstanceOf[ProxyFinalModifier]
    )) /* final 0-arg defs on traits are considered convenience impls (like other arities), not coordinate (property) values */ ) &&
    Modifier.isPublic(m.getModifiers) &&
    !(Modifier.isStatic(m.getModifiers)) &&
    !(Void.TYPE.equals(m.getReturnType)) &&
    !isPreDefMethod(m) && !m.getName.contains(
      "$"
    ) /* all our generated methods have $ in them... user properties never do */ && ! {
      m.getName == "getCallbacks" && m.getReturnType.isArray &&
      m.getReturnType.getComponentType.getName.startsWith("net.sf.cglib")
    } && m.getDeclaringClass != classOf[EntityImpl]
    // all methods declated in EntityImpl should be ignored as they are not properties
  }

  private[optimus] def isPreDefMethod(m: Method): Boolean = {
    val name = m.getName
    predefMethods.contains(name) || name.startsWith("copy$default")
  }

  private[optimus] val predefMethods =
    Set(
      "hashCode",
      "toString",
      "productIterator",
      "productElements",
      "productElementNames",
      "getClass",
      "productPrefix",
      "productArity") ++
      List(classOf[Entity], classOf[Object]).flatMap(_.getMethods).map(_.getName)

  private val tuplePat = "Tuple[1-9]+".r
  private[tree] def typedName(name: String, typeParams: Seq[TypeInfo[_]]) = typeParams match {
    case Seq() => name
    case _ =>
      val ts = typeParams.map(_.name).mkString(", ")
      name match {
        case tuplePat(_*) => s"($ts)"
        case _            => s"${name}[${ts}]"
      }
  }
}

object GenericTypeInfoCnt {
  private val cnt = new AtomicInteger(0)
  def next = cnt.incrementAndGet
}

@parallelizable
object TypeInfo {

  def apply(clazz: Class[_], arguments: TypeInfo[_]*): TypeInfo[_] = {
    if (arguments.isEmpty)
      javaTypeInfo(clazz)
    else
      javaTypeInfo(clazz).copy(typeParams = arguments)
  }

  def underlying(shape: TypeInfo[_]): TypeInfo[_] = {
    if (shape.clazz == classOf[Option[_]] || shape.clazz == classOf[Some[_]]) underlying(shape.typeParams.head)
    else shape
  }

  final def isOption(shape: TypeInfo[_]): Boolean = {
    shape <:< classOf[Option[_]]
  }

  def isCollection(shape: TypeInfo[_]): Boolean = {
    val underlying = TypeInfo.underlying(shape)
    val clz = underlying.clazz
    clz.isArray() || underlying <:< classOf[Iterable[_]]
  }

  def replaceUnderlying(shape: TypeInfo[_], newUnderlying: TypeInfo[_]): TypeInfo[_] = {
    if (shape.clazz == classOf[Option[_]] || shape.clazz == classOf[Some[_]])
      javaTypeInfo(classOf[Option[_]]).copy(typeParams = List(replaceUnderlying(shape.typeParams.head, newUnderlying)))
    else
      newUnderlying
  }

  def typeEquals(t1: TypeInfo[_], t2: TypeInfo[_]): Boolean = {
    if (t1 eq t2) true
    else {
      t1.classes == t2.classes && t1.typeParams.size == t2.typeParams.size && t1.typeParams.zip(t2.typeParams).forall {
        t =>
          typeEquals(t._1, t._2)
      }
    }
  }

  def defaultValue(t: TypeInfo[_]): Any = {
    t match {
      case BOOLEAN => false
      case INT     => 0
      case DOUBLE  => 0.0
      case FLOAT   => 0.0f
      case LONG    => 0L
      case SHORT   => 0.toShort
      case CHAR    => 0.toChar
      case BYTE    => 0.toByte
      case _       => if (t.runtimeClass == classOf[Option[_]]) None else null
    }
  }
  // this can be used for cases (e.g. Tables) which don't require true type info

  private[this] val noInfoVal = new TypeInfo[Object](Nil, Nil, Nil, Nil) { override def runtimeClassName = "<no info>" }
  def noInfo[T]: TypeInfo[T] = noInfoVal.asInstanceOf[TypeInfo[T]]

  /**
   * For query: Query[T], untypedQuery: Query[DynamicObject] = query.untype and ti: TypeInfo[T], dynamicInfo(ti)
   * provides runtime type information for columns of untypedQuery. It allows to treat Query[T] and Query[DynamicObject]
   * uniformly in places that make use of the query's accompanying TypeInfo.
   *
   * NOTE: once a "dynamic" TypeInfo obtained via this method is associated with a Query[DynamicObject], there is no
   * mechanism to keep it in sync with the query as it gets transformed via PriQL operations that can potentially modify
   * the query's underlying coordinates, and this can lead to inconsistencies. Therefore, this method shouldn't be used
   * with dynamic queries that are expected to be freely transformed via PriQL and whose shape is not guaranteed to
   * remain constant for the lifetime of the query.
   *
   * In fact, this method was created with one purpose in mind: enabling serialization of Query[DynamicObject] .
   */
  def dynamicInfo(ti: TypeInfo[_]): TypeInfo[DynamicObject] =
    new TypeInfo[DynamicObject](
      // ti.isDynamic means that ti is either an instance of the base TypeInfo with DynamicObject among its classes,
      // or it has been previously created via dynamicInfo and has a custom implementation of propertyValue and
      // findAllPropertyMethod - in either case, we return a TypeInfo instance with those methods overridden
      if (ti.isDynamic) ti.classes else ti.classes :+ classOf[DynamicObject],
      ti.pureStructuralMethods,
      ti.primaryConstructorParams,
      ti.typeParams
    ) {
      // since the underlying properties are all already available via DynamicObject#getAll, they can be accessed
      // directly and there is no need for reflection
      override def propertyValue(name: String, obj: Any): Any =
        obj match {
          case dyn: DynamicObject => dyn.get(name)
          case _ =>
            throw new IllegalArgumentException(s"Dynamic TypeInfo expects a DynamicObject, received: ${obj.getClass}")
        }

      override protected def findAllPropertyMethod(clazz: Class[_], ignoreProxyFinal: Boolean): Map[String, Method] =
        if (clazz == classOf[DynamicObject]) Map.empty
        else TypeInfoUtils.findAllPropertyMethod(clazz, ignoreProxyFinal)
    }
  implicit val BOOLEAN: TypeInfo[Boolean] = new TypeInfo[Boolean](Seq(classOf[Boolean]), Nil, Nil, Nil)
  // for List[Nothing] to find Nothing's TypeInfo[Nothing] during macro through inferImplicitValue, it will call summon[T] but WeakTypeTag is WeakTypeTag[T], so we need to provide TypeInfo[Nothing] explicitly
  implicit val NOTHING: TypeInfo[Nothing] = new TypeInfo[Nothing](Seq(classOf[Nothing]), Nil, Nil, Nil)
  implicit val INT: TypeInfo[Int] = new TypeInfo[Int](Seq(classOf[Int]), Nil, Nil, Nil)
  implicit val STRING: TypeInfo[String] = new TypeInfo[String](Seq(classOf[String]), Nil, Nil, Nil)
  implicit val DOUBLE: TypeInfo[Double] = new TypeInfo[Double](Seq(classOf[Double]), Nil, Nil, Nil)
  implicit val FLOAT: TypeInfo[Float] = new TypeInfo[Float](Seq(classOf[Float]), Nil, Nil, Nil)
  implicit val CHAR: TypeInfo[Char] = new TypeInfo[Char](Seq(classOf[Char]), Nil, Nil, Nil)
  implicit val BYTE: TypeInfo[Byte] = new TypeInfo[Byte](Seq(classOf[Byte]), Nil, Nil, Nil)
  implicit val LONG: TypeInfo[Long] = new TypeInfo[Long](Seq(classOf[Long]), Nil, Nil, Nil)
  implicit val SHORT: TypeInfo[Short] = new TypeInfo[Short](Seq(classOf[Short]), Nil, Nil, Nil)
  implicit val ANY: TypeInfo[Any] = new TypeInfo[Any](Seq(classOf[Any]), Nil, Nil, Nil)
  implicit val UNIT: TypeInfo[Unit] = new TypeInfo[Unit](Seq(classOf[BoxedUnit]), Nil, Nil, Nil)
  implicit val NULL: TypeInfo[Null] = new TypeInfo[Null](Seq(classOf[Null$]), Nil, Nil, Nil)
  val ITERABLE = new TypeInfo[Iterable[_]](Seq(classOf[Iterable[_]]), Nil, Nil, Seq(TypeInfo.ANY))
  val SET = new TypeInfo[Set[_]](Seq(classOf[Set[_]]), Nil, Nil, Seq(TypeInfo.ANY))

  def toTypeInfo(tpe: JavaReflectType): TypeInfo[_] = {
    if (tpe == null) TypeInfo.NOTHING
    else
      tpe match {
        case p: JavaParameterizedType =>
          val raw = toTypeInfo(p.getRawType)
          TypeInfo(raw.classes, Nil, Nil, p.getActualTypeArguments.map(toTypeInfo(_)))
        case r: TypeInfoRefinedType =>
          val parentTypeInfos = r.parentTypes.map(toTypeInfo)
          val classes = parentTypeInfos.flatMap(_.classes)
          val typeParams = parentTypeInfos.flatMap(_.typeParams)
          TypeInfo(classes, Nil, Nil, typeParams)
        case c: Class[_] => TypeInfo.javaTypeInfo(c)
      }
  }

  def mock(
      className: String,
      pureStructuralMethods: Seq[Signature] = Seq.empty,
      primaryConstructorParams: Seq[(String, Class[_])] = Seq.empty,
      typeParams: Seq[TypeInfo[_]] = Seq.empty
  ): TypeInfo[Any] = {
    new TypeInfo[Any](Nil, pureStructuralMethods, primaryConstructorParams, typeParams) {
      override def runtimeClassName = className
    }
  }

  val ttCache = new ConcurrentHashMap[TypeInfo[_], TypeInfo[_]]()
  val TYPEINFO = TypeInfo.javaTypeInfo(classOf[TypeInfo[_]])

  def generic(value: Any): TypeInfo[_] =
    try {
      if (value == null) ANY
      else
        value match {
          case t: TypeInfo[_] =>
            ttCache.computeIfAbsent(t, t => TYPEINFO.copy(typeParams = List(t)))
          case _: Int     => INT
          case _: String  => STRING
          case _: Boolean => BOOLEAN
          case _: Double  => DOUBLE
          case _: Short   => SHORT
          case _: Long    => LONG
          case _: Char    => CHAR
          case _: Byte    => BYTE
          case _          => TypeInfo.javaTypeInfo(value.getClass)
        }
    } catch { case _: Throwable => null }

  implicit def summon[T]: TypeInfo[T] = macro SummonMacro.summonMacro[T]

  def mkTuple(types: List[TypeInfo[_]]): TypeInfo[_] = {
    val base = TypeInfo.javaTypeInfo(Class.forName(s"scala.Tuple${types.size}"))
    base.copy(typeParams = types)
  }

  def cast(any: String, typeInfo: TypeInfo[_]): Any = {
    if (any == null) any
    else {
      val name = typeInfo.name
      if (name == "int" || name == "Int" || name == "scala.Int" || classOf[Integer].getName().equals(name))
        Integer.valueOf(any)
      else if (
        name == "short" || name == "Short" || name == "scala.Short" || classOf[java.lang.Short]
          .getName()
          .equals(name)
      ) java.lang.Short.valueOf(any)
      else if (
        name == "byte" || name == "Byte" || name == "scala.Byte" || classOf[java.lang.Byte]
          .getName()
          .equals(name)
      ) java.lang.Byte.valueOf(any)
      else if (
        name == "boolean" || name == "Boolean" || name == "scala.Boolean" || classOf[java.lang.Boolean]
          .getName()
          .equals(name)
      ) java.lang.Boolean.valueOf(any)
      else if (
        name == "float" || name == "Float" || name == "scala.Float" || classOf[java.lang.Float]
          .getName()
          .equals(name)
      ) java.lang.Float.valueOf(any)
      else if (
        name == "double" || name == "Double" || name == "scala.Double" || classOf[java.lang.Double]
          .getName()
          .equals(name)
      ) java.lang.Double.valueOf(any)
      else any
    }
  }

  def javaTypeInfo[T](cls: Class[T]): TypeInfo[T] = {
    val cached = TypeInfoCache.javaCache.get(cls)

    if (cached != null) cached.cast[T]
    else {
      val created = new TypeInfo[T](Seq(cls), Nil, Nil, Nil)
      TypeInfoCache.javaCache.putIfAbsent(cls, created)
      created
    }
  }

  def fromManifest[T](mf: Manifest[T]): TypeInfo[T] = {
    val erased = mf.runtimeClass
    val tParams = mf.typeArguments.map(fromManifest(_))
    new TypeInfo[T](Seq(erased), Nil, Nil, tParams)
  }

  @tailrec private def buildIntersection(base: TypeInfo[_], classes: List[Class[_]]): TypeInfo[_] = classes match {
    case Nil                      => base
    case headClass :: tailClasses => buildIntersection(intersect(base, javaTypeInfo(headClass)), tailClasses)
  }

  def intersectionJavaTypeInfo(classes: List[Class[_]]): TypeInfo[_] = classes match {
    case headClass :: tailClasses => buildIntersection(javaTypeInfo(headClass), tailClasses)
    case _                        => throw new IllegalArgumentException("Cannot build TypeInfo with empty class list")
  }

  def getCommonLST(left: TypeInfo[_], right: TypeInfo[_]): TypeInfo[_] = {
    val leftClasses = left.classes.flatMap(findAllSupertypes).distinct ++ left.classes
    val rightClasses = right.classes.flatMap(findAllSupertypes).distinct ++ right.classes
    val commonClasses = leftClasses.intersect(rightClasses)
    val reduce = eliminateSupertypes(commonClasses)
    val local = reduce.filterNot(isBuiltIn)
    val (interfaces, concretes) = local.partition(_.isInterface)
    val candidate = new TypeInfo(concretes ++ interfaces, Nil, Nil, Nil)
    candidate
  }
  def isBuiltIn(cls: Class[_]): Boolean = cls.getName.startsWith("java") || cls.getName.startsWith("scala")

  def intersect[A, B](a: TypeInfo[A], b: TypeInfo[B]): TypeInfo[A with B] = {
    val key = (a, b)
    val cached = TypeInfoCache.intersectionCache.get(key)

    if (cached != null) cached.cast[A with B]
    else {
      val created = createNormalized[A with B](
        a.concreteClass.orElse(b.concreteClass).toSeq ++ Set(a, b).flatMap(_.interfaces).toSeq,
        Set(a, b).flatMap(_.pureStructuralMethods).toSeq,
        a.concreteClass.map(_ => a.primaryConstructorParams).getOrElse(b.primaryConstructorParams),
        List(a, b).flatMap(_.typeParams)
      )

      TypeInfoCache.intersectionCache.putIfAbsent(key, created)
      created
    }
  }

  def intersect[A, B, C](a: TypeInfo[A], b: TypeInfo[B], c: TypeInfo[C]): TypeInfo[A with B with C] =
    intersect[A with B, C](intersect[A, B](a, b), c)

  def createNormalized[T](
      classes: Seq[Class[_]],
      pureStructuralMethods: Seq[Signature],
      primaryConstructorParams: Seq[(String, Class[_])],
      typeParams: Seq[TypeInfo[_]]
  ): TypeInfo[T] = {
    val (interfaces, concretes) = classes.partition(_.isInterface)
    val sorted = concretes.distinct.sortBy(_.getName) ++ interfaces.distinct.sortBy(_.getName)
    val normalized = eliminateSupertypes(sorted)
    val candidate =
      new TypeInfo[T](normalized, pureStructuralMethods.sortBy(_.getName), primaryConstructorParams, typeParams)

    // intern the candidate (to save memory - especially when deserializing
    val previous = TypeInfoCache.interningCache.putIfAbsent(candidate, candidate)
    if (previous != null) previous.cast[T] else candidate
  }

  def createTypeInfo[T](
      classes: Seq[Class[_]],
      primaryConstructorParams: Seq[(String, Class[_])],
      typeParams: Seq[TypeInfo[_]]
  ): TypeInfo[T] = {
    val key = (classes, typeParams)
    AsyncCollectionHelpers.inlineRT {
      val cached = TypeInfoCache.macroGenCache.get(key)
      if (cached != null) cached.cast[T]
      else {
        val created = TypeInfo.createNormalized[T](classes, Nil, primaryConstructorParams, typeParams)
        TypeInfoCache.macroGenCache.putIfAbsent(key, created)
        created
      }
    }
  }

  /**
   * removes any classes in the input sequence which are a supertype of any other class in the list. preserves the
   * ordering of the original list
   */
  def eliminateSupertypes(clss: Seq[Class[_]]): Seq[Class[_]] = {
    val allSupertypes = clss.flatMap(findAllSupertypes).toSet
    clss.filterNot(allSupertypes)
  }

  /**
   * finds all superclasses and interfaces of the cls (all the way up the heirarchy)
   */
  def findAllSupertypes(cls: Class[_]): Set[Class[_]] = {
    val immediate = Option(cls.getSuperclass).toSet ++ cls.getInterfaces
    val nextLevel = immediate.flatMap(findAllSupertypes)
    immediate ++ nextLevel
  }

  final class SummonMacro(override val c: whitebox.Context) extends SummonImpl(c) {
    def summonMacro[T: c.WeakTypeTag]: c.Tree = this.summon(c.weakTypeOf[T])
  }
  class SummonImpl[C <: blackbox.Context](val c: C) {
    import c.universe._
    import internal._

    def PriqlSignature = Ident(typeOf[Signature.type].termSymbol)
    def PriqlTypeInfoParameterizedType = Ident(typeOf[TypeInfoParameterizedType.type].termSymbol)
    def PriqlTypeInfoRefinedType = Ident(typeOf[TypeInfoRefinedType.type].termSymbol)
    def PriqlTypeInfo = Ident(typeOf[TypeInfo.type].termSymbol)
    val TypeInfoCls = symbolOf[TypeInfo[_]]
    lazy val AnyClass = {
      definitions.ClassClass.info // SIDE EFFECT LOAD CLASSCLASS BECAUSE toType WON'T
      definitions.ClassClass.toType match { // tpe_*
        case tp @ TypeRef(_, _, tparam :: Nil) =>
          existentialType(tparam.typeSymbol :: Nil, tp)
        case tp =>
          tp
      }
    }

    def mkList(tpe: Type, elems: List[c.Tree]): c.Tree = {
      internal.gen.mkMethodCall(
        internal.gen.mkAttributedIdent(definitions.ListModule),
        definitions.List_apply,
        tpe :: Nil,
        elems
      )
    }
    def mkList(elems: List[c.Tree]): c.Tree = {
      internal.gen.mkMethodCall(
        internal.gen.mkAttributedIdent(definitions.ListModule),
        definitions.List_apply,
        Nil,
        elems
      )
    }

    def reifyRuntimeClass(tpe: Type): Tree =
      q"${Literal(Constant(tpe))}"

    def findImplicitGenericTypeInfo(tpe: Type, withMacrosDisabled: Boolean): Tree = {
      val pt = appliedType(TypeInfoCls, tpe :: Nil)
      val result = c.inferImplicitValue(pt, silent = true, withMacrosDisabled = true).orElse {
        // avoid going through macro infra again; assume we're the only materializer
        if (!withMacrosDisabled) summon(tpe) else EmptyTree
      }
      // avoid returning "t" for "implicit val t: TypeInfo[T] = typeInfo[T]"
      val ownerSym = c.internal.enclosingOwner
      val resultSym = result.symbol

      if (resultSym == ownerSym || isAccessor(resultSym, ownerSym))
        EmptyTree
      else
        result
    }

    private def isAccessor(maybeAccessor: Symbol, maybeAccessed: Symbol): Boolean =
      maybeAccessor != null && maybeAccessor.isMethod &&
        maybeAccessor.asMethod.isAccessor && maybeAccessor.asMethod.accessed == maybeAccessed

    def summon(tpe: Type): c.Tree = {
      val tpeTree = TypeTree(tpe)
      val typeInfoTree = findImplicitGenericTypeInfo(tpe, true) orElse {

        // return list of TypeInfo[_] tree
        def genTypeArgumentGenericTypeInfos(tpe: Type): List[Tree] = {
          tpe match {
            case _: SingleType => Nil // fine - objects have no type args anyway

            case TypeRef(_, tSym, tArgs) =>
              if (isSkolem(tSym) || (tSym.isAbstract && !tSym.isClass))
                c.abort(
                  c.enclosingPosition,
                  s"Can't create GenericTypeInfo from type $tpe because it's an abstract or a skolem type"
                )

              for (arg <- tArgs) yield {
                // N.B. we allow macro expansion so we may be called recursively
                if (arg.takesTypeArgs) {
                  // higherkind
                  val PolyType(typeParams, resultType) = arg.etaExpand
                  // Literal(Constant(tpe)) == classOf[tpe]; see gen.mkClassOf
                  val clazz = Literal(Constant(internal.existentialType(typeParams, resultType)))
                  val typeArgs = typeParams.map(t => q"$PriqlTypeInfo.noInfo")
                  q"$PriqlTypeInfo.javaTypeInfo($clazz).copy(typeParams = ${mkList(typeArgs)})"
                } else
                  findImplicitGenericTypeInfo(arg, false) orElse {
                    c.abort(
                      c.enclosingPosition,
                      s"Can't create GenericTypeInfo from type $tpe because it has abstract or skolem argument $arg"
                    )
                  }
              }

            case _ => Nil
          }
        }

        def genStructuralMethodInfo(pureStructuralMethods: List[MethodSymbol]) = {
          val trees: List[Tree] = pureStructuralMethods.map { sym =>
            val paramTypes = sym.paramLists.flatten.map(_.typeSignature)
            val returnType = sym.returnType
            val returnClass = reifyRuntimeClass(returnType)
            val richReturnType = extractJavaType(returnType)
            val argumentClasses = paramTypes.map { t =>
              reifyRuntimeClass(t)
            }
            val argumentClassesSeq = mkList(AnyClass, argumentClasses)
            val name = Literal(Constant(sym.name.toString))
            q"$PriqlSignature.apply($name, $returnClass, $argumentClassesSeq, $richReturnType)"
          }

          mkList(trees)
        }

        // return type is the tree of Seq[(String, Class[_])]
        def extractConstructorParams(tpe: Type) = {
          val sym = tpe.decl(termNames.CONSTRUCTOR)

          val exprs =
            if (sym == NoSymbol) Nil
            else {
              val ctor = if (sym.asTerm.isOverloaded) sym.asTerm.alternatives.head.asMethod else sym.asMethod

              ctor.paramLists.flatten map { p =>
                val clazz = reifyRuntimeClass(p.typeSignature)
                q"(${p.name.toString}, $clazz)"
              }
            }

          mkList(appliedType(definitions.TupleClass(2), definitions.StringClass.toType :: AnyClass :: Nil), exprs)
        }

        // return type is tree of JavaReflectType
        def extractJavaType(tpe: Type, knownTypes: Map[Symbol, Tree] = Map.empty): Tree = {
          if (tpe =:= typeOf[Any]) reifyRuntimeClass(typeOf[Any])
          else
            knownTypes.getOrElse(
              tpe.typeSymbol, {
                tpe.dealias match {
                  case dealiased @ TypeRef(_, _, realArgTypes) =>
                    val javaCls = reifyRuntimeClass(dealiased)
                    realArgTypes match {
                      case Nil => javaCls
                      case args =>
                        val argTrees = args.map(a => extractJavaType(a, knownTypes))
                        val argsList = mkList(argTrees)
                        q"$PriqlTypeInfoParameterizedType.apply($argsList, $javaCls)"
                    }
                  case TypeBounds(lo, hi)        => extractJavaType(hi, knownTypes)
                  case t @ SingleType(_, symbol) => extractJavaType(symbol.typeSignature, knownTypes)
                  case ExistentialType(quantified, underlying) =>
                    val quantifiedJavaTypes: Map[Symbol, Tree] =
                      quantified.iterator.map(sym => sym -> extractJavaType(sym.typeSignature, knownTypes)).toMap
                    extractJavaType(underlying, quantifiedJavaTypes ++ knownTypes)
                  case t @ RefinedType(parents, _) =>
                    val parentTrees = parents.map(p => extractJavaType(p))
                    val parentList = mkList(parentTrees)
                    q"$PriqlTypeInfoRefinedType.apply($parentList)"
                }
              }
            )
        }

        tpe match {
          case RefinedType(h1 :: h2 :: tail, decls) =>
            val t1 = findImplicitGenericTypeInfo(h1, false).orElse {
              c.abort(
                c.enclosingPosition,
                s"Can't create GenericTypeInfo from type $tpe because it has abstract or skolem argument"
              )
            }

            // decls does not contain structural types defined in t1 .
            val rightTpe =
              if (tail.isEmpty && decls.isEmpty) h2 else refinedType(h2 :: tail, NoSymbol, decls, c.enclosingPosition)

            val t2 = findImplicitGenericTypeInfo(rightTpe, false).orElse {
              c.abort(
                c.enclosingPosition,
                s"Can't create GenericTypeInfo from type $tpe because it has abstract or skolem argument"
              )
            }

            q"$PriqlTypeInfo.intersect($t1, $t2).cast[$tpeTree]"

          case _ =>
            val argumentGenericTypeInfos = genTypeArgumentGenericTypeInfos(tpe)

            val (types, structuralMethods) = ReflectUtils.extractRealTypesAndStructuralMethods(c.universe)(tpe)

            val filteredTypes = types.filterNot(t => t =:= typeOf[Any] || t.typeSymbol == definitions.ObjectClass)

            val runtimeClasses: Tree = { // tree of Seq[Class[_]]
              val classes: List[Tree] = filteredTypes.map(reifyRuntimeClass(_))
              mkList(AnyClass, classes)
            }

            // XXX Filter out overridden methods in nominal types
            val pureStructuralMethods = structuralMethods.filter(_.overrides.isEmpty)

            val ctorParams = extractConstructorParams(tpe)

            val typeParamsList = mkList(argumentGenericTypeInfos)

            if (pureStructuralMethods.isEmpty) {
              q"$PriqlTypeInfo.createTypeInfo[$tpeTree]($runtimeClasses, $ctorParams, $typeParamsList)"

            } else {
              // we don't bother with caching for structural methods yet... it's a rarer case
              val signatures = genStructuralMethodInfo(pureStructuralMethods)
              q"$PriqlTypeInfo.createNormalized[$tpeTree]($runtimeClasses, $signatures, $ctorParams, $typeParamsList)"

            }

        }
      }
      typeInfoTree
    }
  }
}

object TypeInfoCache {
  // using java CHM for high performance
  import java.util.concurrent.ConcurrentHashMap
  val intersectionCache = new ConcurrentHashMap[(TypeInfo[_], TypeInfo[_]), TypeInfo[_]]()
  val macroGenCache = new ConcurrentHashMap[(Seq[Class[_]], Seq[TypeInfo[_]]), TypeInfo[_]]()
  val javaCache = new ConcurrentHashMap[Class[_], TypeInfo[_]]()
  val interningCache = new ConcurrentHashMap[TypeInfo[_], TypeInfo[_]]()
  val clazzMethodNamesCache = new ConcurrentHashMap[Class[_], collection.Set[String]]

  def clazzMethodNames(c: Class[_]) = {
    val mArray: Array[Method] = c.getMethods
    val mSet = new mutable.HashSet[String]
    var i = 0
    while (i < mArray.length) {
      mSet += mArray(i).getName
      i += 1
    }
    mSet
  }

}

object TupleTypeInfo {
  val PairClass = classOf[Tuple2[_, _]]
  def unapply(ti: TypeInfo[_]): Option[(TypeInfo[_], TypeInfo[_])] = ti match {
    case TypeInfo(PairClass +: _, _, _, Seq(lt, rt)) => Some((lt, rt))
    case _                                           => None
  }
  def apply(lt: TypeInfo[_], rt: TypeInfo[_])(implicit tupleTypeInfo: TypeInfo[(_, _)]) =
    tupleTypeInfo.copy(typeParams = Seq(lt, rt))

}

class InvocationException(name: String, obj: Any, cause: Option[Throwable] = None)
    extends RuntimeException(s"Method '$name' does not exist on object '$obj' of type ${obj.getClass}", cause.orNull)

final case class TypeInfoParameterizedType(typeArgs: Seq[JavaReflectType], getRawType: JavaReflectType)
    extends JavaParameterizedType {
  val getActualTypeArguments: Array[JavaReflectType] = typeArgs.toArray
  val getOwnerType: JavaReflectType = null
  override def toString = "" + getRawType + (if (typeArgs.isEmpty) "" else typeArgs.mkString("[", ", ", "]"))
}

final case class TypeInfoRefinedType(parentTypes: List[JavaReflectType]) extends JavaReflectType

// When we want to take reflectLock in this class, we will need to always take "this" lock inside it.
@nowarn("msg=10003")
case class TypeInfo[T] private[optimus] (
    classes: Seq[Class[_]],
    pureStructuralMethods: Seq[Signature],
    primaryConstructorParams: Seq[(String, Class[_])],
    typeParams: Seq[TypeInfo[_]]
) {
  type Type = T

  lazy val _hashCode = ScalaRunTime._hashCode(this)
  override def hashCode() = _hashCode
  private val vclasses = classes.toArray
  private val vmethods = pureStructuralMethods.toArray

  // wrapped so it does not get auto wrapped to perform operations such as map/exists
  private val clsArr: mutable.WrappedArray[Class[_]] = mutable.WrappedArray.make(vclasses)

  //  lazy val name: String = (typedName +: interfaces.map(_.getName)).mkString(" with ") + structuralMembersName
  lazy val name: String = {
    ((classes map (_.getName)) match {
      case head +: rest => (TypeInfoUtils.typedName(head, typeParams) +: rest).mkString(" with ")
      case Seq()        => runtimeClassName
    }) + structuralMembersName
  }

  lazy val abbreviatedName: String = {
    ((classes map (_.getName)) match {
      case head +: rest =>
        (TypeInfoUtils.typedName(head, typeParams) +: rest).map(_.split("\\.").last).mkString(" + ")
      case Seq() => runtimeClassName
    }) + structuralMembersName
  }

  def runtimeClassName: String = classes.headOption match {
    case Some(cls) => cls.getName
    case None      => "AnyRef"
  }

  @transient lazy val concreteClass = classes.find(!_.isInterface)
  @transient lazy val interfaces = classes.filter(_.isInterface)

  private def structuralMembersName =
    if (pureStructuralMethods.isEmpty) ""
    else
      pureStructuralMethods map { s =>
        "def " + s.getName + ":" + s.getReturnType.getClassName
      } mkString (" { ", "; ", " }")

  override def toString = "typeInfo[" + name + "]"

  val id: Int = GenericTypeInfoCnt.next
  def runtimeClass: Class[Type] =
    (concreteClass ++ interfaces).headOption.map(_.asInstanceOf[Class[Type]]).getOrElse(null)
  val clazz = runtimeClass

  lazy val nodeProperties: Map[String, Field] = {
    val allProps: Map[String, Method] = classes.iterator.flatMap(_.getMethods.toList).map(m => (m.getName, m)).toMap
    // for each of the methods, search a method which has $queued appended to it.
    // if found, use that method instead
    for ((key, method) <- nonStructuralProperties) yield {
      allProps.get(key + "$queued") match {
        case Some(nodeMethod) => (key, OptimusNodeField(nodeMethod))
        case None             => (key, RegularField(method))
      }
    }
  }

  def isDynamic = this <:< classOf[DynamicObject]

  protected def findAllPropertyMethod(clazz: Class[_], ignoreProxyFinal: Boolean): Map[String, Method] =
    TypeInfoUtils.findAllPropertyMethod(clazz, ignoreProxyFinal)

  val nonStructuralProperties: Map[String, Method] =
    classes.iterator.flatMap(c => findAllPropertyMethod(c, true)).toMap

  val allNonStructuralProperties: Map[String, Method] =
    classes.iterator.flatMap(c => findAllPropertyMethod(c, true)).toMap

  private def getPropertyNames(
      ignoreProxyFinal: Boolean,
      propertyNamesLambda: Map[String, Method] => Seq[String],
      structuredPropertyNamesFilter: Signature => Boolean
  ) = {
    val nonStructuralPropertyNames =
      classes.flatMap(c => propertyNamesLambda(findAllPropertyMethod(c, ignoreProxyFinal)))
    val structuralPropertyNames = pureStructuralMethods.withFilter(structuredPropertyNamesFilter).map(_.getName).sorted
    (nonStructuralPropertyNames ++ structuralPropertyNames).distinct
  }

  private def propertyNamesLambda(m: Map[String, Method]): Seq[String] = {
    m.keys.toSeq.sorted
  }

  private def structuredPropertyNamesFilter(m: Signature): Boolean = {
    m.getReturnType != AsmType.VOID && m.getArgumentTypes.length == 0
  }

  // ordered by class (which are already ordered) then by name
  val propertyNames = getPropertyNames(true, propertyNamesLambda, structuredPropertyNamesFilter)

  // ordered by class (which are already ordered) then by name
  val allPropertyNames = getPropertyNames(false, propertyNamesLambda, structuredPropertyNamesFilter)

  private def dimensionPropertyNamesLambda(m: Map[String, Method]): Seq[String] = {
    m.filter { case (_, method) => AnnotationUtils.findAnnotation(method, classOf[dimension]) ne null }
      .keys
      .toSeq
      .sorted
  }

  lazy val dimensionPropertyNames = getPropertyNames(true, dimensionPropertyNamesLambda, s => false)

  @volatile private var propertyMapDefined = false
  private var propertyMapValue: Map[String, TypeInfo[_]] = _
  private def propertyMapCompute(): Map[String, TypeInfo[_]] = {
    this.synchronized {
      if (!propertyMapDefined) {
        propertyMapValue = {
          val propertyParameterizedMap = propertyParameterizedTypes.mapValuesNow(f => TypeInfo.toTypeInfo(f))
          val pureStructuralMethodMap =
            pureStructuralMethods.map(sig => sig.getName -> TypeInfo.toTypeInfo(sig.richReturnType))
          propertyParameterizedMap.++(pureStructuralMethodMap)
        }
        propertyMapDefined = true
      }
    }
    propertyMapValue
  }

  def propertyMap = {
    if (propertyMapDefined) propertyMapValue
    else propertyMapCompute()
  }

  def propertyValue(name: String, obj: Any): Any = {
    // shortcut: if it's non structural we can just return it from our map. else we need to look it up because it can be different for each implementing class.
    try nonStructuralProperties.getOrElse(name, obj.getClass.getMethod(name)).invoke(obj)
    catch {
      case i: IllegalArgumentException  => throw new InvocationException(name, obj, Some(i))
      case i: InvocationTargetException => throw new InvocationException(name, obj, Some(i))
    }
  }

  def typeString = name

  def writeReplace: Object =
    GenericTypeInfoSerializationMemento(
      concreteClass,
      interfaces,
      pureStructuralMethods,
      primaryConstructorParams,
      typeParams
    )

  /**
   * makes a best attempt at getting the (erased) type of a property. only used internally in priql.
   */
  private[tree] def propertyTypeErased(name: String): Option[TypeInfo[_]] = {
    import java.lang.reflect.ParameterizedType
    import java.lang.reflect.TypeVariable

    nonStructuralProperties.get(name) flatMap { method =>
      method.getGenericReturnType match {
        case c: Class[_] => Some(TypeInfo.javaTypeInfo(c))
        case p: ParameterizedType =>
          Some(
            TypeInfo.javaTypeInfo(p.getRawType.asInstanceOf[Class[_]])
          ) // we'll just erase it (that's all we need for now)
        case v: TypeVariable[_] =>
          // if we can guess which type param index this refers to, we can look that up in our own list of type params
          val possibleTypeParams =
            clazz.getTypeParameters().zipWithIndex.filter { case (t, idx) => t.getName == v.getName }
          val possibleTypeParamIdx = possibleTypeParams.map { case (t, idx) => idx }.headOption
          possibleTypeParamIdx flatMap { idx =>
            if (typeParams.isDefinedAt(idx)) Some(typeParams(idx)) else None
          }
        case _ => None
        // we don't support structural types yet...
      }
    }
  }

  private def getPropertyParameterizedTypes(nsp: Map[String, Method]): Map[String, JavaReflectType] = {
    val mirror = RuntimeMirror.forClass(this.getClass)
    classes.iterator.flatMap { cl =>
      val members = mirror.classSymbol(cl).typeSignature.members
      members.collect {
        case method if nsp.contains(method.name.encodedName.toString) =>
          val symbolType = if (method.isMethod) method.asMethod.returnType else method.typeSignature
          method.name.encodedName.toString -> TypeInfoUtils.extractJavaType(symbolType, mirror)
      }
    }.toMap
  }

  @volatile private var propertyParameterizedTypesDefined: Boolean = false
  private var propertyParameterizedTypesValue: Map[String, JavaReflectType] = _
  private def propertyParameterizedTypesCompute(): Map[String, JavaReflectType] = {
    this.synchronized {
      if (!propertyParameterizedTypesDefined) {
        propertyParameterizedTypesValue = getPropertyParameterizedTypes(nonStructuralProperties)
        propertyParameterizedTypesDefined = true
      }
    }
    propertyParameterizedTypesValue
  }

  /**
   * gets un-erased java.lang.reflect.Types for the properties of this type by using Scala runtime reflection (safely)
   */
  def propertyParameterizedTypes: Map[String, JavaReflectType] = {
    if (propertyParameterizedTypesDefined) propertyParameterizedTypesValue
    else propertyParameterizedTypesCompute()
  }

  @volatile private var allPropertyParameterizedTypesDefined: Boolean = false
  private var allPropertyParameterizedTypesValue: Map[String, JavaReflectType] = _
  private def allPropertyParameterizedTypesCompute(): Map[String, JavaReflectType] = {
    this.synchronized {
      if (!allPropertyParameterizedTypesDefined) {
        allPropertyParameterizedTypesValue = getPropertyParameterizedTypes(allNonStructuralProperties)
        allPropertyParameterizedTypesDefined = true
      }
    }
    allPropertyParameterizedTypesValue
  }

  /**
   * gets all un-erased java.lang.reflect.Types for the properties of this type by using Scala runtime reflection
   * (safely)
   */
  def allPropertyParameterizedTypes: Map[String, JavaReflectType] = {
    if (allPropertyParameterizedTypesDefined) allPropertyParameterizedTypesValue
    else allPropertyParameterizedTypesCompute()
  }

  // The accuracy of this method is rather poor. This method is rarely used.
  def <:<(other: TypeInfo[_]): Boolean = (this eq other) || {

    // for all classes in other, we must have a class that is assignable to it
    val isSubclass = other.clsArr.forall(ocl => clsArr.exists(cl => ocl.isAssignableFrom(cl)))

    // really we should check the co/contra-variance of parameters correctly, but for now we'll just
    // assume they are all covariant. also we should correctly handle parameter lists differing between
    // subclasses
    def isParamsMatch = typeParams.size == other.typeParams.size && typeParams.zip(other.typeParams).forall {
      case (t1, t2) => t1 <:< t2
    }

    // for all structural methods in other, we must have a matching (structural or non-structural) method.
    // we should actually check the signatures of those methods, but we don't yet...
    def isStructuralMatch = other.pureStructuralMethods.forall(m => propertyNames.contains(m.getName))

    isSubclass && isParamsMatch && isStructuralMatch
  }

  def >:>(other: TypeInfo[_]): Boolean = other <:< this

  def =:=(other: TypeInfo[_]): Boolean = (this eq other) || (other <:< this && this <:< other)

  def <:<(clazz: Class[_]): Boolean = classes.exists(c => clazz.isAssignableFrom(c))

  def =:=(clazz: Class[_]): Boolean = this <:< clazz && this >:> clazz

  def >:>(clazz: Class[_]): Boolean = {
    var i = 0
    /* Inline equivalent of: classes.forall(c => c.isAssignableFrom(clazz)) &&  */
    while (i < vclasses.length) {
      if (!vclasses(i).isAssignableFrom(clazz))
        return false
      i = i + 1
    }

    /* Inline equivalent of: pureStructuralMethods.forall { p => clazz.getMethods.exists(m => m.getName == p.getName) } */
    val clazzMethodNames =
      TypeInfoCache.clazzMethodNamesCache.computeIfAbsent(clazz, TypeInfoCache.clazzMethodNames)
    i = 0
    while (i < vmethods.length) {
      if (!clazzMethodNames.contains(vmethods(i).name))
        return false
      i = i + 1
    }

    true

  }

  /**
   * unsafe cast operation
   */
  def cast[T]: TypeInfo[T] = asInstanceOf[TypeInfo[T]]
}

/**
 * Wrapper for methods depending on whether they are Nodes or regular methods.
 */
sealed abstract class Field {
  val method: Method
  val isSyncSafe: Boolean
  @nodeSync def invokeOn(instance: Any): Any = needsPlugin
  def invokeOn$queued(instance: Any): NodeFuture[Any]
  def invokeOnSync(instance: Any): Any = ???
}
final case class OptimusNodeField(method: Method) extends Field {
  val isSyncSafe = false
  def invokeOn$queued(instance: Any): NodeFuture[Any] = method.invoke(instance).asInstanceOf[Node[Any]]
}
final case class RegularField(method: Method) extends Field {
  val isSyncSafe = true
  def invokeOn$queued(instance: Any): NodeFuture[Any] = {
    new AlreadyCompletedNode(invokeOnSync(instance))
  }
  override def invokeOnSync(instance: Any): Any = method.invoke(instance)
}
final case class AnonymousField(name: String) extends Field {
  val method = null
  val isSyncSafe = true
  def invokeOn$queued(instance: Any): NodeFuture[Any] = {
    new AlreadyCompletedNode(invokeOnSync(instance))
  }
  override def invokeOnSync(instance: Any): Any = {
    instance.getClass.getMethod(name).invoke(instance)
  }
}

// some of the useful vars in GenericTypeInfo are not serializable, so we'll use this class instead... it just
// capture the constructor params of GenericTypeInfo
final case class GenericTypeInfoSerializationMemento(
    concreteClass: Option[Class[_]],
    interfaces: Seq[Class[_]],
    pureStructuralMethods: Seq[Signature],
    primaryConstructorParams: Seq[(String, Class[_])],
    typeParams: Seq[TypeInfo[_]]
) {

  def readResolve: Object =
    TypeInfo.createNormalized(
      concreteClass.toList ++ interfaces,
      pureStructuralMethods,
      primaryConstructorParams,
      typeParams
    )

}
