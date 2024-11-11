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
package optimus.platform.inputs.loaders

import optimus.graph.GraphException
import optimus.platform.FreezableNodeInputMap
import optimus.platform.inputs.NodeInput
import optimus.platform.inputs.NodeInputMapValue
import optimus.platform.inputs.NodeInputMapValues._
import optimus.platform.inputs.NodeInputs
import optimus.platform.inputs.NodeInputs.ScopedSINodeInput
import optimus.platform.inputs.ProcessSINodeInput
import optimus.platform.inputs.ScopedSIEntry
import optimus.scalacompat.collection._

import java.util.function.BiConsumer

/** Used in [[NodeInputMapOps.foldLeft]] */
final case class FoldedNodeInput(
    nodeInput: NodeInput[AnyRef],
    source: LoaderSource,
    value: AnyRef,
    isLocal: Boolean,
    forwardToEngine: Boolean)

sealed trait NodeInputMap[NI[T] <: NodeInput[T]]

sealed trait NodeInputMapOps[NI[T] <: NodeInput[T], NIM <: NodeInputMap[NI]] { self: NIM =>
  private type VisibleEntry = (NI[AnyRef], VisibleValue[AnyRef])
  private type EngineVisibleEntry = (NI[AnyRef], EngineValue[AnyRef])

  protected val localInputs: Map[NI[_], VisibleValue[_]]
  protected val engineSpecificInputs: Map[NI[_], EngineValue[AnyRef]]

  protected def copyClass(
      localInputs: Map[NI[_], VisibleValue[_]],
      engineSpecificInputs: Map[NI[_], EngineValue[AnyRef]]): NIM

  // typesafe getters
  protected def getLocalInput[T](input: NI[T]): Option[VisibleValue[T]] =
    localInputs.get(input).map(_.asInstanceOf[VisibleValue[T]])
  protected[loaders] def getEngineSpecificInput[T](input: NI[T]): Option[EngineValue[T]] =
    engineSpecificInputs.get(input).map(_.asInstanceOf[EngineValue[T]])

  private[loaders] def appendLocalInput[T](
      input: NI[T],
      value: T,
      source: LoaderSource,
      forwardToEngine: Boolean): NIM = {
    val forwardToEngineModified = forwardToEngine && !NodeInputs.isNeverForwarding(input)
    def update(engineSpecificInputsCopy: Map[NI[_], EngineValue[AnyRef]], forceForwardToEngine: Boolean): NIM =
      getLocalInput(input) match {
        case Some(VisibleValue(currSource, currValue, currForwardToEngine)) =>
          val (newValue, newForwardToEngine, newSource) = {
            val sourcedInput = input.combine(currSource, currValue, source, value)
            // unless forced, preserving the flag ensures that if the input was *not* supposed to be forwarded, any
            // subsequent update keeps the same behaviour. vice versa, if the input was supposed to be forwarded, any subsequent
            // update ensures the input will still be visible on engines
            (sourcedInput.value(), forceForwardToEngine || currForwardToEngine, sourcedInput.source())
          }

          if (newValue == currValue && currForwardToEngine == newForwardToEngine && newSource == currSource) {
            // try to avoid creating copies of the same object
            if (engineSpecificInputs eq engineSpecificInputsCopy) this
            else copyClass(localInputs, engineSpecificInputsCopy)
          } else
            copyClass(
              localInputs.updated(input, VisibleValue(newSource, newValue, newForwardToEngine)),
              engineSpecificInputsCopy)

        case None =>
          // forward to engine if the input is auto-forwarding or it was specified as forwarding
          // (ie: loaded from OPTIMUS_DIST_JAVA_OPTS, or from code), AND the engine doesn't contain
          // a different valued version
          val toForward =
            (NodeInputs.isAlwaysForwarding(input) || forwardToEngineModified) && !engineSpecificInputsCopy
              .contains(input)
          // update the mapping
          copyClass(
            localInputs.updated(input, VisibleValue(source, value, forceForwardToEngine || toForward)),
            engineSpecificInputsCopy)
      }
    getEngineSpecificInput(input) match {
      case Some(EngineValue(_, currValue)) if currValue == value =>
        // if `engineSpecificInputs` already contains a mapping for the same value, remove it from `engineSpecificInputs`
        // and force the mapping in `localInputs` to forward to engine
        // note that the engine cannot have a copy of a NEVER forwarding input so forwarding behavior is consistent
        update(engineSpecificInputs - input, forceForwardToEngine = true)
      case None | Some(_) =>
        // otherwise follow the normal flow
        update(engineSpecificInputs, forceForwardToEngine = false)
    }
  }

  private[loaders] def appendEngineSpecificInput[T](input: NI[T], value: T, source: LoaderSource): NIM = {
    def update(localInputsCopy: Map[NI[_], VisibleValue[_]]): NIM =
      getEngineSpecificInput(input) match {
        case Some(EngineValue(currSource, currValue)) =>
          // if the currently mapped value is identical to the desired one, do nothing
          if (currValue == value) {
            if (localInputs eq localInputsCopy) this
            else copyClass(localInputsCopy, engineSpecificInputs)
          } else {
            // update the mapping
            val sourcedInput = input.combine(currSource, currValue, source, value)
            copyClass(
              localInputsCopy,
              engineSpecificInputs.updated(
                input,
                EngineValue(sourcedInput.source(), sourcedInput.value().asInstanceOf[AnyRef])))
          }
        case None =>
          // update the mapping
          copyClass(
            localInputsCopy,
            engineSpecificInputs.updated(input, EngineValue(source, value.asInstanceOf[AnyRef])))
      }

    localInputs.get(input) match {
      case Some(VisibleValue(currSource, currValue, forwardToEngine)) if currValue == value =>
        val winningSource = input.combine(currSource, currValue.asInstanceOf[T], source, value).source()
        // if there is already an equivalent mapping with `forwardToEngine=true` and equivalent value, then do not
        // do anything as the right value will be forwarded
        if (forwardToEngine && winningSource == currSource) this
        // else flip that to `forwardToEngine=true`
        else
          copyClass(
            localInputs.updated(input, VisibleValue(winningSource, currValue, forwardToEngine = true)),
            engineSpecificInputs)
      case None | Some(VisibleValue(_, _, false)) =>
        // if there is no mapping, or the one present should not forward, then simply store the new engine mapping
        update(localInputs)
      case Some(visValue) =>
        // else the mapping must have `forwardToEngine=true` but with a different value, in which case flip that to
        // false to maintain the invariant. until we expose an API to override engine specific inputs, this should
        // happen if someone loads inputs in the wrong order, so we keep this as a safeguard
        update(localInputs.updated(input, VisibleValue(visValue.source, visValue.value, forwardToEngine = false)))
    }
  }

  def getInput[T](nodeInput: NI[T]): Option[T] = getLocalInput(nodeInput) match {
    case s @ Some(_) => s.map(_.value)
    case None =>
      val dflt = nodeInput.defaultValue()
      if (dflt.isPresent) Some(dflt.get) else None
  }

  private[loaders] def TEST_ONLY_getInputWithSource[T](nodeInput: NI[T]): Option[NodeInputMapValue[T]] = getLocalInput(
    nodeInput)

  protected def localInputsSeq: Seq[VisibleEntry] = localInputs.toSeq.map { case (input, vv) =>
    (input.asInstanceOf[NI[AnyRef]], vv.asInstanceOf[VisibleValue[AnyRef]])
  }
  protected def engineSpecificInputsSeq: Seq[EngineVisibleEntry] = engineSpecificInputs.toSeq.map {
    case (input, value) =>
      (input.asInstanceOf[NI[AnyRef]], value)
  }

  // callback will never be called on any NEVER forwarding inputs
  private[inputs] def engineForwardingForEach(
      callback: BiConsumer[NodeInput[AnyRef], NodeInputMapValue[AnyRef]]): Unit = {
    localInputsSeq.foreach {
      case (input, vv @ VisibleValue(_, value, true)) =>
        getEngineSpecificInput(input) match {
          case Some(otherValue) =>
            throw new GraphException(
              s"Invariant broken, there should never be two conflicting inputs both trying to forward to engine (input=${input
                  .name()} v1=$value, v2=$otherValue)")
          case None =>
            callback.accept(input, vv)
        }
      case _ =>
    }
    engineSpecificInputsSeq.foreach { case (input, value) =>
      callback.accept(input, value)
    }
  }

  private[optimus] def foldLeft[B](initial: B)(op: (B, FoldedNodeInput) => B): B = {
    var accumulator = initial
    foreach(entry => accumulator = op(accumulator, entry))
    accumulator
  }

  private[optimus] def foreach[B](op: FoldedNodeInput => Unit): Unit = {
    localInputsSeq.foreach { case (input, VisibleValue(source, value, forwardToEngine)) =>
      val entry = FoldedNodeInput(input, source, value, isLocal = true, forwardToEngine)
      op(entry)
    }
    engineSpecificInputsSeq.foreach { case (input, EngineValue(source, value)) =>
      val entry = FoldedNodeInput(input, source, value, isLocal = false, forwardToEngine = true)
      op(entry)
    }
  }

  private[optimus] def filterKeys(p: NI[_] => Boolean): NIM =
    copyClass(localInputs.filterKeysNow(p), engineSpecificInputs.filterKeysNow(p))

  protected def getSerialized: Map[NI[_], VisibleValue[_]] = {
    // only java serializable node inputs that forward to engine should be serialized
    var javaSerializableForwarding = Map.empty[NI[_], VisibleValue[_]]
    engineForwardingForEach { (nodeInput, nodeInputMapValue) =>
      if (NodeInputs.isJavaSerialized(nodeInput))
        javaSerializableForwarding = javaSerializableForwarding.updated(
          nodeInput.asInstanceOf[NI[_]],
          VisibleValue(nodeInputMapValue.source, nodeInputMapValue.value, forwardToEngine = true))
    }
    javaSerializableForwarding
  }
}

/**
 * Maps [[NodeInput]] to corresponding values.
 *
 * The implementation guarantees that whenever the same [[NodeInput]] is present in both
 * [[FrozenNodeInputMap.localInputs]] and [[FrozenNodeInputMap.engineSpecificInputs]], the local version is marked as
 * not-forwarding to engines.
 *
 * @see
 *   [[NodeInputStorage]] for a detailed overview of the desired behavior
 *
 * @param localInputs
 *   holds all the [[NodeInput]] which are visible in the local process. Each [[NodeInput]] can optionally be forwarded
 *   to the engine.
 * @param engineSpecificInputs
 *   holds all [[NodeInput]] which are invisible in the local process and will <b>always</b> be forwarded to the engine
 *   after distribution.
 */
final case class FrozenNodeInputMap private (
    override protected val localInputs: Map[ScopedSINodeInput[_], VisibleValue[_]],
    override protected val engineSpecificInputs: Map[ScopedSINodeInput[_], EngineValue[AnyRef]])
    extends FreezableNodeInputMap
    with NodeInputMap[ScopedSINodeInput]
    with NodeInputMapOps[ScopedSINodeInput, FrozenNodeInputMap] {
  override def copyClass(
      localInputs: Map[ScopedSINodeInput[_], VisibleValue[_]],
      engineSpecificInputs: Map[ScopedSINodeInput[_], EngineValue[AnyRef]]): FrozenNodeInputMap =
    copy(localInputs, engineSpecificInputs)

  private[optimus] def isEmpty: Boolean = localInputs.isEmpty && engineSpecificInputs.isEmpty

  // inputs added to the map with these functions will be forwaded to engine
  def withExtraInput[T](input: ScopedSINodeInput[T], value: T): FrozenNodeInputMap =
    appendLocalInput(input, value, LoaderSource.CODE, forwardToEngine = true)

  private def withExtraInput[T](input: ScopedSINodeInput[T], value: T, source: LoaderSource) =
    appendLocalInput(input, value, source, forwardToEngine = true)
  private def withExtraInput[T](entry: ScopedSIEntry[T]): FrozenNodeInputMap =
    withExtraInput(entry.nodeInput, entry.value, entry.sourcedInput().source())
  def withExtraInputs(entries: Seq[ScopedSIEntry[_]]): FrozenNodeInputMap = entries.foldLeft(this) { (curr, entry) =>
    curr.withExtraInput(entry)
  }

  def withoutInput[T](input: ScopedSINodeInput[T]): FrozenNodeInputMap = copy(localInputs = localInputs - input)

  def keyValuePairs: Seq[ScopedSIEntry[_]] = localInputsSeq.map { case (input, vv) =>
    ScopedSIEntry(input, vv.value)
  }

  private def writeReplace(): AnyRef = {
    val nonTransients = getSerialized
    if (nonTransients.isEmpty) FrozenNodeInputMap.empty
    else FrozenNodeInputMapMarker(nonTransients)
  }

  override def freeze: FrozenNodeInputMap = this

  override def contains[T](input: ScopedSINodeInput[T]): Boolean = localInputs.contains(input)

  def mergeWith(other: FrozenNodeInputMap): FrozenNodeInputMap = {
    var curr = this
    other.localInputsSeq.foreach { case (input, VisibleValue(source, value, forwardToEngine)) =>
      curr = curr.appendLocalInput(input, value, source, forwardToEngine = forwardToEngine)
    }
    other.engineSpecificInputsSeq.foreach { case (input, EngineValue(source, value)) =>
      curr = curr.appendEngineSpecificInput(input, value, source)
    }
    curr
  }
}

private final case class FrozenNodeInputMapMarker(localInputs: Map[ScopedSINodeInput[_], VisibleValue[_]]) {
  private def readResolve(): AnyRef = FrozenNodeInputMap(localInputs, Map.empty)
}

object FrozenNodeInputMap {
  val empty: FrozenNodeInputMap = FrozenNodeInputMap(Map.empty, Map.empty)
}

final case class ProcessSINodeInputMap private (
    override protected[inputs] val localInputs: Map[ProcessSINodeInput[_], VisibleValue[_]],
    override protected[inputs] val engineSpecificInputs: Map[ProcessSINodeInput[_], EngineValue[AnyRef]])
    extends NodeInputMap[ProcessSINodeInput]
    with NodeInputMapOps[ProcessSINodeInput, ProcessSINodeInputMap] {
  override def copyClass(
      localInputs: Map[ProcessSINodeInput[_], VisibleValue[_]],
      engineSpecificInputs: Map[ProcessSINodeInput[_], EngineValue[AnyRef]]): ProcessSINodeInputMap =
    copy(localInputs, engineSpecificInputs)

  private def writeReplace(): AnyRef = {
    val nonTransients = getSerialized
    if (nonTransients.isEmpty) ProcessSINodeInputMap.empty
    else ProcessSINodeInputMapMarker(nonTransients)
  }

  private[inputs] def getInputWithoutDefault[T](input: ProcessSINodeInput[T]): Option[T] =
    getLocalInput(input).map(_.value)

  private[inputs] def getInputSourceWithoutDefault[T](input: ProcessSINodeInput[T]): Option[LoaderSource] =
    getLocalInput(input).map(_.source)

  private[optimus] def mergeWith(other: ProcessSINodeInputMap): ProcessSINodeInputMap = {
    var curr = this
    other.localInputsSeq.foreach { case (input, VisibleValue(source, value, forwardToEngine)) =>
      curr = curr.appendLocalInput(input, value, source, forwardToEngine = forwardToEngine)
    }
    other.engineSpecificInputsSeq.foreach { case (input, EngineValue(source, value)) =>
      curr = curr.appendEngineSpecificInput(input, value, source)
    }
    curr
  }
}

private final case class ProcessSINodeInputMapMarker(localInputs: Map[ProcessSINodeInput[_], VisibleValue[_]]) {
  private def readResolve(): AnyRef = ProcessSINodeInputMap(localInputs, Map.empty)
}

object ProcessSINodeInputMap {
  val empty: ProcessSINodeInputMap = ProcessSINodeInputMap(Map.empty, Map.empty)
}
