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
package optimus.breadcrumbs.graph

import spray.json._
import optimus.breadcrumbs.crumbs.Properties.JsonImplicits._

import scala.collection.mutable
import java.time.ZonedDateTime

import msjava.base.util.uuid.MSUuid
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.EdgeType
import optimus.breadcrumbs.crumbs.Event
import optimus.breadcrumbs.crumbs.Events
import optimus.breadcrumbs.crumbs.Properties
import spray.json.JsValue

import scala.annotation.tailrec
import scala.collection.immutable.SortedSet

object Vertex {

  import DefaultJsonProtocol._

  final case class Edge(tpe: EdgeType.EdgeType, vuid: MSUuid) extends Ordered[Edge] {
    override def compare(that: Edge): Int = vuid.toString.compare(that.vuid.toString)
    var vo: Option[Vertex] = None
    override def toString = s"E($tpe, $vuid, ${vo.isDefined})"
    override def equals(obj: Any): Boolean = obj match {
      case e: Edge => e.tpe == tpe && e.vuid == vuid
      case _       => false
    }
    override def hashCode(): Int = tpe.hashCode() ^ vuid.hashCode()
  }

  // Note that we don't persist vertex pointer!
  implicit object EdgeJsonFormat extends JsonFormat[Edge] {
    override def write(obj: Edge): JsValue = (obj.tpe.toString, obj.vuid).toJson
    override def read(json: JsValue): Edge = {
      val (tpe, vuid) = json.convertTo[(String, MSUuid)]
      Edge(EdgeType.withName(tpe), vuid)
    }
  }

  // Reify the edge links
  def connect(vs: Seq[Vertex]): Unit = {
    val vmap = mutable.Map.empty[MSUuid, Vertex]
    vs.foreach { v =>
      vmap += ((v.vuid, v))
    }
    vs.foreach { v =>
      v.children.foreach { e =>
        { e.vo = vmap.get(e.vuid) }
      }
      v.parents.foreach { e =>
        { e.vo = vmap.get(e.vuid) }
      }
    }
  }

  // This magic lets us convert between Vertex and JSON.  The trailing integer must match the number of
  // arguments of the Vertex case class.
  implicit val vertexFormat: RootJsonFormat[Vertex] = jsonFormat9(Vertex.apply)

  def merge(fname: String): Iterable[Vertex] = {
    val cm = new CrumbMerger
    cm.merge(fname)
  }

  final case class NameTree(name: String, kids: Map[NameTree, Int]) {
    override def toString: String = name + kids.mkString("<<", ", ", ">>")
    def outline(): Unit = {
      def outline(depth: Int, n: Int, nt: NameTree): Unit = {
        println("  " * depth + n + "x " + nt.name)
        for ((k, nk) <- nt.kids) {
          outline(depth + 1, nk, k)
        }
      }
      outline(0, 1, this)
    }
  }
  object NameTree {

    def log10Compress(i: Int): Int = Math.pow(10, Math.log10(0.5 + i).toInt).toInt

    def apply(name: String, kids: Seq[NameTree], compress: Int => Int = log10Compress): NameTree = {
      val s = kids.foldLeft(Map.empty[NameTree, Int]) { (m, nt) =>
        m.get(nt)
          .fold {
            m + ((nt, 1))
          } { i =>
            m + ((nt, i + 1))
          }
      }
      new NameTree(name, s.mapValues(compress(_)).toMap)
    }
  }

  /**
   * Given an input Vertex iterator prev v0, v1, v2, .... Return the concatenation of iterators f(v0), f(v1), f(v2), ...
   */
  private class NestedIterator(prev: Iterator[Vertex], f: Vertex => Iterator[Vertex]) extends Iterator[Vertex] {
    var i: Iterator[Vertex] = Iterator.empty
    var cachedNext: Option[Option[Vertex]] = None
    def calcNext(): Option[Vertex] = {
      while (cachedNext.isEmpty) {
        if (i.hasNext) {
          cachedNext = Some(Some(i.next()))
          cachedNext.get
        } else if (prev.hasNext) {
          i = f(prev.next())
        } else {
          cachedNext = Some(None)
        }
      }
      cachedNext.get
    }
    override def hasNext: Boolean = calcNext().isDefined
    override def next(): Vertex = {
      val ret = calcNext().get
      cachedNext = None
      ret
    }
  }

  /**
   * Given an input iterator prev that produces vertices along with some arbitrary accumulated result: (v0, r0), (v1,
   * r1), .... Produce another iterator that is the concatenation of iterators f(v0).map { v => (v,rf(r0,v))}, f(v1).map
   * { v => (v,rf(r1,v))}, ... So, if the output of f(v_i) is v_i0, v_i1, v_i2, ... We'll have the concatenation of
   * iterators producing (v_00,rf(r0,v_00) , (v_01,rf(r0,v01), .... (v_10,rf(r1,v_10) , (v_11,rf(r1,v11), ....
   */
  private class NestedAccumulatingIterator[R](
      prev: Iterator[(Vertex, R)],
      f: Vertex => Iterator[Vertex],
      rf: (R, Vertex) => R)
      extends Iterator[(Vertex, R)] {
    var i: Iterator[(Vertex, R)] = Iterator.empty
    var cachedNext: Option[Option[(Vertex, R)]] = None
    def calcNext(): Option[(Vertex, R)] = {
      while (cachedNext.isEmpty) {
        if (i.hasNext) {
          cachedNext = Some(Some(i.next()))
          cachedNext.get
        } else if (prev.hasNext) {
          val (vp, rp) = prev.next()
          i = f(vp).map { v =>
            (v, rf(rp, v))
          }
        } else {
          cachedNext = Some(None)
        }
      }
      cachedNext.get
    }
    override def hasNext: Boolean = calcNext().isDefined
    override def next(): (Vertex, R) = {
      val ret = calcNext().get
      cachedNext = None
      ret
    }
  }

  @scala.annotation.tailrec
  def nestedIterator(prev: Iterator[Vertex], fs: List[Vertex => Iterator[Vertex]]): Iterator[Vertex] = fs match {
    case f :: frest =>
      nestedIterator(new NestedIterator(prev, f), frest)
    case _ =>
      prev
  }

  @scala.annotation.tailrec
  def nestedAccumulatingIterator[R](
      prev: Iterator[(Vertex, R)],
      fs: List[Vertex => Iterator[Vertex]],
      rf: (R, Vertex) => R): Iterator[(Vertex, R)] = fs match {
    case f :: frest =>
      nestedAccumulatingIterator(new NestedAccumulatingIterator(prev, f, rf), frest, rf)
    case _ =>
      prev
  }

  // Intermediate class for accumulating a sequence of iterator-creators fs
  class Navigator(v0: Vertex, fs: Seq[Vertex => Iterator[Vertex]]) {

    def seek(
        f: Vertex => Boolean,
        pred: EdgeType.EdgeType => Boolean = { _ =>
          true
        },
        down: Boolean = true,
        maxDepth: Int = Int.MaxValue) =
      new Navigator(
        v0,
        fs :+ { v: Vertex =>
          v.seek(f, pred, down, maxDepth)
        })
    def stalk(
        f: Vertex => Boolean,
        avoid: Vertex => Boolean,
        pred: EdgeType.EdgeType => Boolean = { _ =>
          true
        },
        down: Boolean = true,
        maxDepth: Int = Int.MaxValue) =
      new Navigator(
        v0,
        fs :+ { v: Vertex =>
          v.stalk(f, avoid, pred, down, maxDepth)
        })
    def next(
        pred: EdgeType.EdgeType => Boolean = { _ =>
          true
        },
        down: Boolean = true) =
      new Navigator(
        v0,
        fs :+ { v: Vertex =>
          v.next(pred, down)
        })

    // Return the final value along the path of iterator-creators
    def targets: Iterator[Vertex] = nestedIterator(Seq(v0).iterator, fs.toList)

    // Return the vertex path corresponding to the path of iterator-creators
    def paths: Iterator[Seq[Vertex]] = {
      def iacc: Iterator[(Vertex, Seq[Vertex])] =
        nestedAccumulatingIterator[Seq[Vertex]](
          Seq((v0, Seq.empty)).iterator,
          fs.toList,
          (vs: Seq[Vertex], v: Vertex) => vs :+ v)
      iacc.map(_._2)
    }

    /*
     E.g. find all DAL requests that are exactly one level of disting away
        v.nav.seek(_.has(P.distedTo)).next().stalk(_.has(P.requestId), _.has(P.distedTo)).targets
     */

  }

  // Random utilities
  def zdtToMs(zdt: ZonedDateTime): Long = zdt.toInstant.toEpochMilli

}

// Remember to update Vertex.vertexFormat if changing the number of arguments!
final case class Vertex(
    uuid: ChainedID,
    vuid: MSUuid,
    tFirst: ZonedDateTime,
    tLast: ZonedDateTime,
    events: Seq[(ZonedDateTime, Event)],
    children: SortedSet[Vertex.Edge],
    parents: SortedSet[Vertex.Edge],
    logs: Seq[(ZonedDateTime, Map[Properties.Key[_], JsValue])],
    properties: Map[Properties.Key[_], JsValue]) {

  import Vertex._

  def this(uuid: String, vuid: String, t: Long) = {
    this(
      ChainedID.parse(uuid, vuid),
      new MSUuid(vuid),
      CrumbMerger.toZDT(t),
      CrumbMerger.toZDT(t),
      Seq.empty,
      children = SortedSet.empty,
      parents = SortedSet.empty,
      logs = Seq.empty,
      properties = Map.empty
    )
  }

  def withTime(t: ZonedDateTime): Vertex = {
    if (t.isBefore(tFirst))
      copy(tFirst = t)
    else if (t.isAfter(tLast))
      copy(tLast = t)
    else this
  }

  private[graph] def withTime(t: Long): Vertex = withTime(CrumbMerger.toZDT(t))

  def name: String = get(Properties.name).getOrElse("<unknown>")
  override def toString = s"V($uuid, $name)"

  // Retrieve value from nested map of strings
  def get(ks: String*): Option[String] = properties.gets(ks: _*)
  def has(ks: String*): Boolean = properties.has(ks: _*)

  // Retrieve a properly typed property.
  def get[A](k: Properties.Key[A]): Option[A] = properties.getp[A](k)
  def getAs[A: JsonReader](k: String): Option[A] = properties.getAs[A](k)
  def getEvent(e: String): Option[JsValue] = properties.get(Events.parseEvent(e))
  def has(k: Properties.Key[_]): Boolean = properties.has(k)
  def apply[A](k: Properties.Key[A]): A = get(k).get
  def apply(k: String*): String = get(k: _*).get

  def compress(
      fn: Vertex => String = _.name,
      pred: EdgeType.EdgeType => Boolean = { _ =>
        true
      },
      down: Boolean = true): NameTree = {

    val v2n = mutable.Map.empty[MSUuid, NameTree]
    val n2v = mutable.Map.empty[NameTree, Vertex]

    def mkTree(v: Vertex): NameTree = {
      v2n.getOrElse(
        v.vuid, {
          val ng = (if (down) v.children else v.parents).toSeq
            .filter { e =>
              pred(e.tpe)
            }
            .map(_.vo.get)
          val t = NameTree(v.name, ng.map(mkTree))
          v2n += ((v.vuid, t))
          n2v += ((t, v))
          t
        }
      )
    }
    mkTree(this)
  }

  /**
   * Iterate along a depth first traversal.
   * @param pred
   *   \- predicate for edge type (default all)
   * @param down
   *   \- true (default) for following children, false for follow parents
   * @param maxDepth
   *   \- (default infinite)
   * @param term
   *   \- if set, predicate for terminating traversal.
   * @return
   */
  def iterator(
      pred: EdgeType.EdgeType => Boolean = { _ =>
        true
      },
      down: Boolean = true,
      maxDepth: Int = Int.MaxValue,
      term: Vertex => Boolean = { _ =>
        false
      }): Iterator[Vertex] = new Iterator[Vertex] {
    private val seen = mutable.Set.empty[MSUuid]
    private var stack: List[(Vertex, Int)] = List((Vertex.this, 0))
    private def pushChildren(v: Vertex, i: Int): Unit =
      if (i <= maxDepth && !term(v)) {
        unsorted(if (down) v.children else v.parents)
          .filter { e =>
            pred(e.tpe) && !seen(e.vuid)
          }
          .flatMap { _.vo }
          .toSeq
          .sortBy(_.uuid.toString)
          .reverse
          .foreach { vv =>
            seen += vv.vuid
            stack = (vv, i) :: stack
          }
      }
    override def hasNext: Boolean = stack.nonEmpty
    override def next(): Vertex = {
      val (v, i) = stack.head
      stack = stack.tail
      pushChildren(v, i + 1)
      v
    }
  }

  def nav = new Navigator(this, Seq.empty)

  def up(): Iterable[Vertex] = unsorted(parents).map(_.vo.get)
  def up(pred: EdgeType.EdgeType => Boolean): Iterable[Vertex] =
    unsorted(parents).filter(e => pred(e.tpe)).map(_.vo.get)
  def down(): Iterable[Vertex] = unsorted(children).map(_.vo.get)
  def down(pred: EdgeType.EdgeType => Boolean): Iterable[Vertex] =
    unsorted(children).filter(e => pred(e.tpe)).map(_.vo.get)
  @tailrec
  private def iPath(vs: Vector[Vertex], is: List[Int]): Iterable[Vertex] = {
    if (is.isEmpty)
      vs
    else {
      val is1 = is.head
      val ng = unsorted(if (is1 > 0) vs.last.children else vs.last.parents).map(_.vo.get)
      val i = Math.abs(is1)
      if (ng.size < i)
        vs
      else
        iPath(vs :+ ng.drop(i - 1).head, is.tail)
    }
  }

  /**
   * Return the sequence of vertices obtained by following a path of i'th children (if i>0) or parents (if i<0). The
   * first child of course is 1. Note that edges are represented internally as an ordered set, so use of this function
   * should be reproducible. * @param is
   * @return
   */
  def iPath(is: Int*): Iterable[Vertex] = iPath(Vector(this), is.toList)

  def foldLeft[Z](z: Z)(
      f: (Z, Vertex) => Z,
      pred: EdgeType.EdgeType => Boolean = { _ =>
        true
      },
      down: Boolean = true,
      maxDepth: Int = Int.MaxValue): Z =
    iterator(pred, down, maxDepth).foldLeft(z)(f)

  def foreach(
      f: Vertex => Unit,
      pred: EdgeType.EdgeType => Boolean = { _ =>
        true
      },
      down: Boolean = true,
      maxDepth: Int = Int.MaxValue): Unit =
    iterator(pred, down, maxDepth).foreach(f)

  def collect[A](
      pf: PartialFunction[Vertex, A],
      pred: EdgeType.EdgeType => Boolean = { _ =>
        true
      },
      down: Boolean = true,
      maxDepth: Int = Int.MaxValue): Iterable[A] =
    iterator(pred, down, maxDepth).collect(pf).toIterable

  def find(
      f: Vertex => Boolean,
      pred: EdgeType.EdgeType => Boolean = { _ =>
        true
      },
      down: Boolean = true,
      maxDepth: Int = Int.MaxValue): Option[Vertex] =
    iterator(pred, down).find(f)

  def seek(
      f: Vertex => Boolean,
      pred: EdgeType.EdgeType => Boolean = { _ =>
        true
      },
      down: Boolean = true,
      maxDepth: Int = Int.MaxValue): Iterator[Vertex] =
    iterator(pred, down, maxDepth, f).filter(f)

  def stalk(
      f: Vertex => Boolean,
      avoid: Vertex => Boolean,
      pred: EdgeType.EdgeType => Boolean = { _ =>
        true
      },
      down: Boolean = true,
      maxDepth: Int = Int.MaxValue): Iterator[Vertex] =
    iterator(
      pred,
      down,
      maxDepth,
      { v =>
        f(v) || avoid(v)
      }).filter(f)

  def next(
      pred: EdgeType.EdgeType => Boolean = { _ =>
        true
      },
      down: Boolean = true): Iterator[Vertex] = iterator(pred, down, 1).drop(1)

  def filter(
      f: Vertex => Boolean,
      pred: EdgeType.EdgeType => Boolean = { _ =>
        true
      },
      down: Boolean = true,
      maxDepth: Int = Int.MaxValue): Iterator[Vertex] =
    iterator(pred, down, maxDepth).filter(f)
  private def unsorted[A](s: scala.collection.immutable.SortedSet[A]): scala.collection.immutable.Set[A] = s
}
