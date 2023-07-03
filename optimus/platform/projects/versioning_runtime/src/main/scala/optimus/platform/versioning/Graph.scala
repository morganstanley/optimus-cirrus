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
package optimus.platform.versioning

import java.util.concurrent.ConcurrentHashMap
import java.util.Collections

import scala.jdk.CollectionConverters._
import scala.collection.mutable

private[optimus] trait Vertex[A] {
  val data: A
}

private[optimus] trait Edge[A, V <: Vertex[A]] {
  val weight: Edge.Weight
  val source: V
  val destination: V
}

private[optimus] object Edge {
  type Weight = Int
}

private[optimus] trait Graph[A, V <: Vertex[A], E <: Edge[A, V]] {
  private[this] val _vertices: mutable.Set[V] =
    Collections.newSetFromMap(new ConcurrentHashMap[V, java.lang.Boolean]).asScala
  private[this] val _shortestPaths: mutable.Map[(V, V), Seq[E]] = new ConcurrentHashMap[(V, V), Seq[E]].asScala
  private[this] val _outboundEdges: mutable.Map[V, Set[E]] = new ConcurrentHashMap[V, Set[E]].asScala
  private[this] val _inboundEdges: mutable.Map[V, Set[E]] = new ConcurrentHashMap[V, Set[E]].asScala

  private[optimus] def clear(): Unit = {
    _vertices.clear()
    _outboundEdges.clear()
    _shortestPaths.clear()
    _inboundEdges.clear()
  }

  def addEdge(e: E) = {
    _vertices.add(e.source)
    _vertices.add(e.destination)
    _outboundEdges.put(e.source, _outboundEdges.getOrElse(e.source, Set.empty[E]) + e)
    _inboundEdges.put(e.destination, _inboundEdges.getOrElse(e.destination, Set.empty[E]) + e)
    addShortestPaths(e.source)
    addShortestPaths(e.destination)
  }

  private[this] def addShortestPaths(v: V) = {
    val otherVerts = _vertices filterNot { o =>
      v == o
    }
    otherVerts foreach { other =>
      dijkstra(v, other) foreach { _shortestPaths.update((v, other), _) }
      dijkstra(other, v) foreach { _shortestPaths.update((other, v), _) }
    }
  }

  def getAllInboundEdges(v: V): Seq[E] = {
    val queue = new mutable.Queue[V]
    val inBoundEdges = new mutable.ListBuffer[E]
    val processed = new mutable.HashSet[V]
    queue.enqueue(v)
    while (queue.nonEmpty) {
      val destination = queue.dequeue()
      val edges = _inboundEdges.get(destination).getOrElse(Set.empty)
      edges.foreach(edge => {
        if (!processed.contains(edge.source)) {
          queue.enqueue(edge.source)
          inBoundEdges += edge
        }
      })
      processed += destination
    }
    inBoundEdges
  }

  def shortestPaths = _shortestPaths.toMap

  def shortestPath(from: V, to: V) = _shortestPaths.get(from, to)

  // Uses Dijkstra's algorithm to find the shortest path between two nodes in the graph (if there is one)
  private[this] def dijkstra(start: V, end: V): Option[Seq[E]] = {
    if (_vertices.contains(start) && _vertices.contains(end)) {
      val distances = mutable.HashMap[V, Edge.Weight](start -> 0)
      // it might be worth switching this to a better priority queue implementation
      val queue = mutable.HashSet[V](start)
      val settled = mutable.HashSet.empty[V]
      val predecessors = mutable.HashMap.empty[V, E]

      var reachedDestination = false
      while (!queue.isEmpty && !reachedDestination) {
        val closestVertex = {
          val queueIterator = queue.iterator
          queueIterator.foldLeft(queueIterator.next()) { (u, v) =>
            if (distances(u) <= distances(v)) u else v
          }
        }
        queue -= closestVertex
        settled += closestVertex

        reachedDestination = closestVertex == end

        if (!reachedDestination) {
          for (e <- _outboundEdges.getOrElse(closestVertex, Set()) if !settled.contains(e.destination)) {
            val updatedDist = distances(closestVertex) + e.weight
            if (!distances.contains(e.destination) || updatedDist < distances(e.destination)) {
              distances += e.destination -> updatedDist
              predecessors += e.destination -> e
              queue += e.destination
            }
          }
        }
      }

      if (!reachedDestination) None
      else {
        val path = mutable.ListBuffer.empty[E]
        var v = end
        while (v != start) {
          val e = predecessors(v)
          path.+=:(e)
          v = e.source
        }
        Some(path.toSeq)
      }
    } else None
  }
}
