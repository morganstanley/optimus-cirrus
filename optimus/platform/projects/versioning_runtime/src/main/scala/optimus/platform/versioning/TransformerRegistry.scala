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

import optimus.platform._
import optimus.platform.annotations.deprecating
import optimus.platform.internal.SimpleStateHolder
import optimus.platform.storable.SerializedEntity

import scala.util.Try
import scala.util.Success
import scala.util.Failure
import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.internal.SimpleGlobalStateHolder
import optimus.platform.pickling.PickledProperties

import scala.jdk.CollectionConverters._
import java.util.{concurrent => juc}
import scala.collection.mutable

private[versioning] class TransformerRegistryState {
  import TransformerRegistry._

  val writeShapes = new juc.ConcurrentHashMap[SerializedEntity.TypeRef, Map[Int, Shape]] asScala
  val forcedReadTransforms = new juc.ConcurrentHashMap[SerializedEntity.TypeRef, Transformer.Direction] asScala
  val forcedWriteTransforms = new juc.ConcurrentHashMap[SerializedEntity.TypeRef, Transformer.Direction] asScala
  val canonicalShapes = new juc.ConcurrentHashMap[SerializedEntity.TypeRef, Shape] asScala

  val clientGraph = ClientGraph(Set.empty[ClientVertex], Set.empty[ClientEdge])
  val serverGraph = ServerGraph(Set.empty[ServerVertex], Set.empty[ServerEdge])

  def debugString: String = this.synchronized {
    val clientPathStrings = clientGraph.shortestPaths.values.map { path =>
      (path.head.source.data +: path.map(_.destination.data)).mkString(" -> ")
    }
    val serverPathStrings = serverGraph.shortestPaths.values.map { path =>
      (path.head.source.data +: path.map(_.destination.data)).mkString(" -> ")
    }
    val ret = new StringBuilder
    ret.append("Transformer registry state:")
    ret.append("\nwriteShapes:")
    if (writeShapes.isEmpty) ret.append(" <empty>") else ret.append(s"\n  ${writeShapes.mkString("\n  ")}")
    ret.append("\nforcedReadTransforms:")
    if (forcedReadTransforms.isEmpty) ret.append(" <empty>")
    else ret.append(s"\n  ${forcedReadTransforms.mkString("\n  ")}")
    ret.append("\nforcedWriteTransforms:")
    if (forcedWriteTransforms.isEmpty) ret.append(" <empty>")
    else ret.append(s"\n  ${forcedWriteTransforms.mkString("\n  ")}")
    ret.append("\ncanonicalShapes:")
    if (canonicalShapes.isEmpty) ret.append(" <empty>") else ret.append(s"\n  ${canonicalShapes.mkString("\n  ")}")
    ret.append("\nclientShortestPaths:")
    if (clientPathStrings.isEmpty) ret.append(" <empty>") else ret.append(s"\n  ${clientPathStrings.mkString("\n  ")}")
    if (serverPathStrings.isEmpty) ret.append(" <empty>") else ret.append(s"\n ${serverPathStrings.mkString("\n  ")}")
    ret.result()
  }
}

/**
 * General locking strategy:
 *   - methods which result in updates to the registry's mutable state are synchronized on the entire state. This is
 *     probably overkill but there is likely to be little contention on those APIs. It is necessary because with MT
 *     access to the registry we need a consistent view of the world for any given request to version some properties.
 *   - reads are not synchronized on the grounds that -- provided some ordering is maintained when updating data
 *     structures during write -- they will always have a consistent enough view of the world between subsequent
 *     accesses not to get an inconsistent answer. All the shared data structures are concurrent in order to avoid the
 *     need to synchronize on the entire state during reads.
 */
object TransformerRegistry extends SimpleGlobalStateHolder(() => new TransformerRegistryState) {
  private[this] val log = getLogger(this)

  private[optimus] sealed trait TransformerEdge { self: Edge[_, _] =>
    def operation: Transformer.Direction
  }

  private[optimus] final case class ClientVertex(data: Shape) extends Vertex[Shape] {
    override lazy val hashCode = data.hashCode()
  }
  private[optimus] final case class ClientEdge(
      source: ClientVertex,
      destination: ClientVertex,
      operation: Transformer.Direction,
      val addedFieldMap: PickledProperties = PickledProperties.empty,
      val hasRename: Boolean = false,
      val isSafe: Boolean = false,
      weight: Int = -1)
      extends Edge[Shape, ClientVertex]
      with TransformerEdge
  private[optimus] final case class ClientGraph(vertices: Set[ClientVertex], edges: Set[ClientEdge])
      extends Graph[Shape, ClientVertex, ClientEdge]

  private[optimus] final case class ServerVertex(data: RftShape) extends Vertex[RftShape] {
    override lazy val hashCode = data.hashCode()
  }
  private[optimus] final case class ServerEdge(
      source: ServerVertex,
      destination: ServerVertex,
      operation: Transformer.Direction,
      weight: Int = -1)
      extends Edge[RftShape, ServerVertex]
      with TransformerEdge
  private[optimus] final case class ServerGraph(vertices: Set[ServerVertex], edges: Set[ServerEdge])
      extends Graph[RftShape, ServerVertex, ServerEdge]

  private[this] def registerCanonicalShape(shape: Shape) = getState.synchronized {
    getState.canonicalShapes.put(shape.className, shape)
    log.debug(s"Registered canonical shape for className ${shape.className} as ${shape}")
  }

  private[this] def registerEdge[A, V <: Vertex[A], E <: Edge[A, V]](e: E, graph: Graph[A, V, E]): Unit = {
    // Ordering here is important: we must update edges in graph to get new shortest paths before adding canonical
    // shapes as reads, which check canonical shape first, are not synchronized and so can race.
    graph.addEdge(e)
    log.debug(s"Registered transformer for ${e.source.data} to ${e.destination.data}")
    if (e.source.data.isInstanceOf[Shape]) {
      val clientSideShape = e.source.data.asInstanceOf[Shape]
      if (clientSideShape.isCanonical) registerCanonicalShape(clientSideShape)
    }
    if (e.destination.data.isInstanceOf[Shape]) {
      val clientSideShape = e.destination.data.asInstanceOf[Shape]
      if (clientSideShape.isCanonical) registerCanonicalShape(clientSideShape)
    }
  }

  private[optimus /*platform*/ ] def register(t: Transformer): Unit = getState.synchronized {
    def registerInverse(
        v1: ServerVertex,
        v2: ServerVertex,
        vv1: ClientVertex,
        vv2: ClientVertex,
        inverse: Transformer.Direction) = {
      val clientInverse = ClientEdge(vv2, vv1, inverse)
      val serverInverse = ServerEdge(v2, v1, inverse)
      registerEdge(clientInverse, getState.clientGraph)
      registerEdge(serverInverse, getState.serverGraph)
    }

    def registerForward(
        v1: ServerVertex,
        v2: ServerVertex,
        vv1: ClientVertex,
        vv2: ClientVertex,
        forward: Transformer.Direction,
        addedFieldMap: PickledProperties = PickledProperties.empty,
        hasRename: Boolean = false,
        isSafe: Boolean = false) = {
      val client = ClientEdge(vv1, vv2, forward, addedFieldMap, hasRename, isSafe)
      val server = ServerEdge(v1, v2, forward)
      registerEdge(client, getState.clientGraph)
      registerEdge(server, getState.serverGraph)
    }

    val v1 = ServerVertex(t.fromRftShape)
    val v2 = ServerVertex(t.toRftShape)
    val vv1 = ClientVertex(t.fromShape)
    val vv2 = ClientVertex(t.toShape)

    t match {
      case i: Inverse =>
        i match {
          case s: SafeTransformer =>
            registerForward(v1, v2, vv1, vv2, s.forwards, s.addedFields, s.renamedFields.nonEmpty, true)
          case _ => registerForward(v1, v2, vv1, vv2, i.forwards)
        }
        registerInverse(v1, v2, vv1, vv2, i.inverse)
      case _ =>
        registerForward(v1, v2, vv1, vv2, t.forwards)
    }
  }

  def forceRegister(t: Transformer): Unit = register(t)

  def registerWriteShape(s: RftShape): Unit = registerWriteShape(s.generateClientSideShape())

  def registerWriteShape(s: RftShape, schemaVersion: Int): Unit =
    registerWriteShape(s.generateClientSideShape(), schemaVersion)

  def registerWriteShape(s: Shape): Unit = getState.synchronized {
    registerWriteShape(s, 0)
  }

  def registerWriteShape(shape: Shape, schemaVersion: Int): Unit = getState.synchronized {
    val shapes = getState.writeShapes.getOrElse(shape.className, Map.empty)
    getState.writeShapes.put(shape.className, shapes + (schemaVersion -> shape))
    log.debug(s"Write shape for class ${shape.className} at schemaVersion $schemaVersion set to $shape")
  }

  @deprecating(
    "Rename a field or add a placeholder rather than forcing transformation. Forced transformers will be removed in future.")
  def forceReadTransformation(clz: SerializedEntity.TypeRef, transform: Transformer.Direction): Unit =
    getState.synchronized {
      getState.forcedReadTransforms.put(clz, transform)
    }

  @deprecating(
    "Rename a field or add a placeholder rather than forcing transformation. Forced transformers will be removed in future.")
  def forceWriteTransformation(clz: SerializedEntity.TypeRef, transform: Transformer.Direction): Unit =
    getState.synchronized {
      getState.forcedWriteTransforms.put(clz, transform)
    }

  def clearOnlyForUnitTestUsage(): Unit = clear()

  private[optimus] def clear(): Unit = getState.synchronized {
    log.debug("Clearing the TransformerRegistry")
    getState.serverGraph.clear()
    getState.clientGraph.clear()
    getState.writeShapes.clear()
    getState.forcedReadTransforms.clear()
    getState.forcedWriteTransforms.clear()
    getState.canonicalShapes.clear()
  }

  def getCanonicalShape(className: String): Option[Shape] = {
    val s = getState
    s.canonicalShapes.get(className)
  }

  def getWriteShapes(clz: SerializedEntity.TypeRef): Option[Map[Int, Shape]] = {
    getState.writeShapes.get(clz)
  }

  @async
  private[optimus] def version(
      data: PickledProperties,
      from: Shape,
      to: Shape,
      loadContext: TemporalContext
  ): Option[PickledProperties] = {
    val v1 = ClientVertex(from)
    val v2 = ClientVertex(to)
    versionImpl(data, getState.clientGraph, v1, v2, loadContext)
  }

  @async
  private[optimus] def versionUsingRegisteredFieldType(
      data: PickledProperties,
      from: RftShape,
      to: RftShape,
      loadContext: TemporalContext
  ): Option[PickledProperties] = {
    val v1 = ServerVertex(from)
    val v2 = ServerVertex(to)
    versionImpl(data, getState.serverGraph, v1, v2, loadContext)
  }

  @async
  private def versionImpl[A, V <: Vertex[A], E <: Edge[A, V] with TransformerEdge](
      data: PickledProperties,
      graph: Graph[A, V, E],
      v1: V,
      v2: V,
      loadContext: TemporalContext
  ): Option[PickledProperties] = {
    val transformers = graph.shortestPath(v1, v2)
    transformers match {
      case None => None
      case Some(ts) => {
        log.debug(s"Versioning ${data} through ${ts.length} transformers")
        val i = ts.toIterator
        var b = data
        while (i.hasNext) {
          b = i.next().operation(b, loadContext)
        }
        Some(b)
      }
    }
  }

  @async
  private[optimus] def versionToWrite(
      className: SerializedEntity.TypeRef,
      properties: PickledProperties,
      slot: Int,
      temporalContext: => TemporalContext
  ): Try[Map[Int, PickledProperties]] = {
    if (!getState.writeShapes.contains(className)) Success(Map(slot -> properties))
    else {
      val currentShape = Shape.fromProperties(className, properties)
      val targetShapes = getState.writeShapes(className)
      val (successes, failures) = targetShapes.foldLeft((Map.empty[Int, PickledProperties], Seq.empty[(Int, Shape)])) {
        case ((successes, failures), (currentSlot, targetShape)) =>
          if (currentShape == targetShape) (successes + (currentSlot -> properties), failures)
          else {
            val versioned = version(properties, currentShape, targetShape, temporalContext)
            versioned map { versionedProperties =>
              (successes + (currentSlot -> versionedProperties), failures)
            } getOrElse {
              (successes, failures :+ (currentSlot -> targetShape))
            }
          }
      }
      if (failures.isEmpty) Success(successes)
      else {
        log.warn(s"Failed to find transformers to version from $currentShape to ${failures.mkString(";")}")
        Failure(new NoTransformerPathException(currentShape, failures.map(_._2)))
      }
    }
  }

  @async
  private[optimus] def executeForcedReadTransform(
      className: SerializedEntity.TypeRef,
      properties: Transformer.Properties,
      temporalContext: => TemporalContext) =
    executeForcedTransform(getState.forcedReadTransforms, className, properties, temporalContext)

  @async
  private[optimus] def executeForcedWriteTransform(
      className: SerializedEntity.TypeRef,
      properties: Transformer.Properties,
      temporalContext: => TemporalContext) =
    executeForcedTransform(getState.forcedWriteTransforms, className, properties, temporalContext)

  @async
  private[this] def executeForcedTransform(
      transforms: mutable.Map[SerializedEntity.TypeRef, Transformer.Direction],
      className: SerializedEntity.TypeRef,
      properties: Transformer.Properties,
      temporalContext: => TemporalContext) = {
    if (!transforms.contains(className)) properties
    else {
      val transform = transforms(className)
      log.debug(s"Force-versioning ${properties}")
      transform(properties, temporalContext)
    }
  }

  def dumpState(): Unit = {
    log.info(getState.debugString)
  }

  private[platform] def getServerGraph = getState.serverGraph
  private[optimus] def getClientGraph = getState.clientGraph
}
