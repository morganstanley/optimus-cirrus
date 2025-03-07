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
package optimus.graph

import java.io.Serializable

/**
 * Contains extra info attached to the node. The design is somewhat insane. We assume that there are three types of
 * extra info:
 *   1. exception: an exception, which might be null 2. warnings: a Set of Warnings 3. info: a pointer to more
 *      NodeExtendedInfo, which points back to this in the base class
 *
 * There are many subclasses of NodeExtendedInfo, but, outside of this file, they are not _supposed_ to override info(),
 * so it continues to be the this pointer. If we have class MyExtendedInfo(val foo: Int) extends NodeExtendedInfo then
 * mei.withException(ex) will return StdNodeExtendedInfo(ex, mei.warnings, mei): NodeExtendedInfo thus preserving the
 * original info.
 *
 * Class possibilities: abstract NodeExtendedInfo
 * \|- object NullNodeExtendedInfo when no exception or warnings, and info eq null
 * \|- object SinkExtendedInfo don't even allow attaching anything
 * \|- RandomUserExtendedInfo no exception or warnings, and info eq this
 * \|- final StdNodeExtendedInfo possibly exception and/or warnings; info points to some RandomUserExtendedInfo
 */
abstract class NodeExtendedInfo extends Serializable {
  /* Feeds back the node information to the NEI it's attached to */
  def nodeCompleted(ntsk: NodeTask, isCancelled: Boolean): Unit = {}

  // The following would be "sealed methods" if those existed.
  // We don't want them extended outside of the file, but we can't make them normal final methods
  // since they are extended within the file, so we make them final, but do the class match here in
  // the base class.
  final def exception: Throwable = this match {
    case std: StdNodeExtendedInfo => std._exception
    case _                        => null
  }
  final def warnings: Set[Warning] = this match {
    case std: StdNodeExtendedInfo => std._warnings
    case _                        => Set.empty
  }
  final def info: NodeExtendedInfo = this match {
    case std: StdNodeExtendedInfo => std._info
    case NullNodeExtendedInfo     => null
    case SinkNodeExtendedInfo     => null
    case _                        => this
  }

  final def withException(ex: Throwable): NodeExtendedInfo = this match {
    case std: StdNodeExtendedInfo =>
      if ((ex eq null) && std._warnings.isEmpty && (std._info eq null))
        NullNodeExtendedInfo
      else if (ex eq std._exception)
        this
      else
        new StdNodeExtendedInfo(ex, std._warnings, std._info)
    case NullNodeExtendedInfo => if (ex eq null) this else new StdNodeExtendedInfo(ex, Set.empty, null)
    case SinkNodeExtendedInfo => this
    case _                    => new StdNodeExtendedInfo(ex, Set.empty, this)
  }

  final def withWarning(warning: Warning): NodeExtendedInfo = this match {
    case std: StdNodeExtendedInfo => new StdNodeExtendedInfo(exception, std._warnings + warning, std._info)
    case NullNodeExtendedInfo     => new StdNodeExtendedInfo(null, Set(warning), null)
    case SinkNodeExtendedInfo     => this
    case _                        => new StdNodeExtendedInfo(null, Set(warning), this)
  }

  final def withInfo(inf: NodeExtendedInfo): NodeExtendedInfo = this match {
    case std: StdNodeExtendedInfo => new StdNodeExtendedInfo(std._exception, std._warnings, inf)
    case NullNodeExtendedInfo     => inf
    case SinkNodeExtendedInfo     => this
    case _                        => inf // effectively throws entire current class away
  }

  final def withoutInfo: NodeExtendedInfo = this match {
    case std: StdNodeExtendedInfo => new StdNodeExtendedInfo(std._exception, std._warnings, null)
    case _                        => NullNodeExtendedInfo
  }

  final def withoutWarnings: NodeExtendedInfo = this match {
    case std: StdNodeExtendedInfo =>
      if ((std._exception eq null) && (std._info eq null))
        NullNodeExtendedInfo
      else
        new StdNodeExtendedInfo(std._exception, Set.empty, std._info)
    case _ => this
  }

  final def withoutExtractedNotes: (Set[Note], NodeExtendedInfo) = this match {
    case std: StdNodeExtendedInfo =>
      val warnings = std.warnings
      if (warnings.isEmpty)
        (Set.empty, this)
      else {
        val (notes, rest) = warnings.partition(_.isInstanceOf[Note])
        (notes.asInstanceOf[Set[Note]], new StdNodeExtendedInfo(std._exception, rest, std._info))
      }
    case _ => (Set.empty, this)
  }

  final def withoutException: NodeExtendedInfo = this match {
    case std: StdNodeExtendedInfo =>
      if ((std._info eq null) && std._warnings.isEmpty)
        NullNodeExtendedInfo
      else if (std._exception eq null)
        this
      else
        new StdNodeExtendedInfo(null, std._warnings, std._info)
    case _ => this
  }

  final def withoutWarningsOrException: NodeExtendedInfo = this match {
    case std: StdNodeExtendedInfo =>
      if (std._info eq null)
        NullNodeExtendedInfo
      else
        new StdNodeExtendedInfo(null, Set.empty, std._info)
    case _ => this
  }

  /**
   * Basic rules: null += null == null A += A == A
   *
   * null += A == A A += null == A
   *
   * A += B != B+=A
   *
   * May return a new or reused object Even though target and result are typed as AnyRef, this method MUST return a
   * subclass of NodeExtendedInfo
   */
  def combine(child: NodeExtendedInfo, ntsk: NodeTask): NodeExtendedInfo = {
    if (child eq null) this
    else {
      val childInfo = child.info // The real info
      val ninfo = if (childInfo eq null) this else merge(childInfo, ntsk)
      val nwarnings = if (child.warnings ne warnings) child.warnings.union(warnings) else warnings
      val nexception = child.exception
      if (nwarnings.isEmpty && (nexception eq null)) ninfo else new StdNodeExtendedInfo(nexception, nwarnings, ninfo)
    }
  }

  // TODO (OPTIMUS-0000): remove this indirection, but that will require a code update
  def merge(child: NodeExtendedInfo, ntsk: NodeTask): NodeExtendedInfo = this + child
  def +(child: NodeExtendedInfo): NodeExtendedInfo =
    throw new UnsupportedOperationException("Have to override either combine or '+'")
}

/**
 * Standard Node Extended Info. Supports errors/warnings and keeps 'other extended info'
 */
final class StdNodeExtendedInfo(val _exception: Throwable, val _warnings: Set[Warning], val _info: NodeExtendedInfo)
    extends NodeExtendedInfo {
  /* Feeds back the node information to the NEI it's attached to */
  def this(ex: Throwable) = this(ex, Set.empty, null)
  def this(warning: Warning) = this(null, Set(warning), null)
  override def nodeCompleted(ntsk: NodeTask, isCancelled: Boolean): Unit =
    if (_info != null) _info.nodeCompleted(ntsk, isCancelled)

  /**
   * See rules from NodeExtendedInfo
   */
  override def combine(child: NodeExtendedInfo, ntsk: NodeTask): NodeExtendedInfo = {
    if (child eq NullNodeExtendedInfo) this
    else {
      val nwarnings = if (child.warnings ne warnings) child.warnings.union(warnings) else _warnings
      val ninfo = if (_info eq null) child.info else _info.combine(child.info, ntsk)
      // If the child had an exception, NodeTask.completeWithException would already have set it in _exception
      if ((ninfo ne _info) || (nwarnings ne _warnings)) new StdNodeExtendedInfo(_exception, nwarnings, ninfo)
      else this
    }
  }
}

class ProfilingNodeExtendedInfo(_exception: Exception, _warnings: Set[Warning], _info: NodeExtendedInfo)

object NullNodeExtendedInfo extends NodeExtendedInfo {
  final override def combine(child: NodeExtendedInfo, ntsk: NodeTask): NodeExtendedInfo = child.withoutException

  private val moniker: AnyRef with Serializable = new Serializable { def readResolve(): AnyRef = NullNodeExtendedInfo }
  // noinspection ScalaUnusedSymbol called via reflection on serialization
  private def writeReplace(): AnyRef = moniker
}

object SinkNodeExtendedInfo extends NodeExtendedInfo {
  final override def combine(child: NodeExtendedInfo, ntsk: NodeTask): NodeExtendedInfo = this
}
