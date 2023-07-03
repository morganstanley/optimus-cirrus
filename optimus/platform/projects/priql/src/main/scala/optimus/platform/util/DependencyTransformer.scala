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
package optimus.platform.util

import scala.collection.mutable.HashMap

import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform.relational.PriqlSettings
import java.util.concurrent.atomic.AtomicInteger

/**
 * The DependencyTransformer will first collect the dependency info and calculate the max depth/reference count. Then we
 * traverse nodes partitioned by depth (from deepest ones). We release the result once reference count reaches 0. For
 * each level(depth), the transform operations could be async.
 */
abstract class DependencyTransformer[From, To]() {
  protected def dependenciesOverride(f: From): IndexedSeq[From]
  @node protected def transformOverride(f: From, s: IndexedSeq[To]): To

  @node final def transform(f: From): To = {
    val info = collectInfo(f)
    val infoGroups = info.values.groupBy(n => n.depth)
    var depth = infoGroups.keys.max
    while (depth >= 0) {
      infoGroups(depth)
        .apar(PriqlSettings.concurrencyLevel)
        .map(inf => {
          val ds = dependenciesOverride(inf.from)
            .map(d => {
              val i = info(d)
              val to = i.to.get
              if (i.refCount.decrementAndGet() <= 0)
                i.to = None
              to
            })
          inf.to = Some(transformOverride(inf.from, ds))
        })
      depth -= 1
    }
    infoGroups(0).head.to.get
  }

  private def collectInfo(f: From): HashMap[From, Node[From, To]] = {
    val info = new HashMap[From, Node[From, To]]
    var stack: List[(From, Int)] = (f, 0) :: Nil

    while (stack.nonEmpty) {
      val (t, depth) = stack.head
      stack = stack.tail
      val node = info.getOrElseUpdate(t, new Node[From, To](t))
      node.depth = Math.max(node.depth, depth)
      val visitedBefore = node.visited
      node.visited = true

      dependenciesOverride(t).foreach(f => {
        if (!visitedBefore) {
          val n = info.getOrElseUpdate(f, new Node[From, To](f))
          n.refCount.incrementAndGet()
        }
        stack = (f, depth + 1) :: stack
      })
    }
    info
  }

  private class Node[From, To](val from: From) {
    val refCount = new AtomicInteger(0)
    var depth = 0
    var visited = false
    var to: Option[To] = None
  }
}

abstract class DependencyTransformerOffGraph[From, To]() {
  protected def dependenciesOverride(f: From): IndexedSeq[From]
  protected def transformOverride(f: From, s: IndexedSeq[To]): To

  final def transform(f: From): To = {
    val info = collectInfo(f)
    val infoGroups = info.values.groupBy(n => n.depth)
    var depth = infoGroups.keys.max
    while (depth >= 0) {
      infoGroups(depth).map(inf => {
        val ds = dependenciesOverride(inf.from).map(d => {
          val i = info(d)
          val to = i.to.get
          i.refCount -= 1
          if (i.refCount <= 0)
            i.to = None
          to
        })
        inf.to = Some(transformOverride(inf.from, ds))
      })
      depth -= 1
    }
    infoGroups(0).head.to.get
  }

  private def collectInfo(f: From): HashMap[From, Node[From, To]] = {
    val info = new HashMap[From, Node[From, To]]
    var stack: List[(From, Int)] = (f, 0) :: Nil

    while (stack.nonEmpty) {
      val (t, depth) = stack.head
      stack = stack.tail
      val node = info.getOrElseUpdate(t, new Node[From, To](t))
      node.depth = Math.max(node.depth, depth)
      val visitedBefore = node.visited
      node.visited = true

      dependenciesOverride(t).foreach(f => {
        if (!visitedBefore) {
          val n = info.getOrElseUpdate(f, new Node[From, To](f))
          n.refCount += 1
        }
        stack = (f, depth + 1) :: stack
      })
    }
    info
  }

  private class Node[From, To](val from: From) {
    var refCount = 0
    var depth = 0
    var visited = false
    var to: Option[To] = None
  }
}
