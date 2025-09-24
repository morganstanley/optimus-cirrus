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
package optimus.graph.tracking

import optimus.graph.OGTrace
import optimus.breadcrumbs.crumbs.ProfiledEventCause

import scala.annotation.tailrec

object ECProfiler {
  private val NanosPerMillis = 1000 * 1000
  sealed trait GlobalEventTagMaybe
  final case class Tagged(depth: Int, tag: String) extends GlobalEventTagMaybe
  final case object NotTagged extends GlobalEventTagMaybe
}

/**
 * ECProfiler holds profiling data for a root event cause and all its children. The data is generally queried when the
 * root event cause is completed, and thus when the profiling info is "finalized" but can be safely snapshotted at any
 * time.
 * @param root
 */
class ECProfiler(root: RootEventCause) { handler =>
  import ECProfiler._

  /** The root profiling leaf. */
  def prof: ECProfilerLeaf = leaf

  /**
   * True if there are children event that have profiling data.
   *
   * NOTE: If only the root has profiling data, this is false! This matters because the UI framework produces events with
   * metadata that we don't want to publish because they aren't user events.
   */
  def hasProfiledChildren: Boolean = _hasProfiledChildren

  // internal state
  private val leaf =
    new LeafImpl(root.cause, root.includeInHandlerProfCrumb, 0)

  // Extracted here just so that we don't have to walk the whole tree whenever we want to know if there is profiling
  // data
  @volatile private var _hasProfiledChildren: Boolean = _
  private var _globalTag: GlobalEventTagMaybe = NotTagged

  // Used in some tests. Note that this isn't synchronized with any snapshot so you shouldn't use it if you care about
  // consistency.
  private[optimus] def globalTag: Option[String] = handler.synchronized {
    _globalTag match {
      case Tagged(_, tag)       => Some(tag)
      case ECProfiler.NotTagged => None
    }
  }

  private def withGlobalTag(pec: ProfiledEventCause) = handler.synchronized {
    _globalTag match {
      case NotTagged      => pec
      case Tagged(_, tag) => pec.copy(profilingData = pec.profilingData ++ Map(EventCause.globalTagKey -> tag))
    }
  }

  def getAllProfilingData: ProfiledEventCause = withGlobalTag(leaf.snap(true))

  // Accumulates data for a given EventCause
  final private class LeafImpl(
      val cause: String,
      val includeInHandlerProfCrumb: Boolean,
      val depth: Int,
  ) extends ECProfilerLeaf {

    override def getSummaryProfilingData: ProfiledEventCause = {
      @tailrec def foreachChild(stack: List[LeafImpl])(f: LeafImpl => Unit): Unit = {
        stack match {
          case head :: rest =>
            f(head)
            foreachChild(head.children ::: rest)(f)
          case Nil => ()
        }
      }

      withGlobalTag(
        snap(false).copy(childEvents = {
          // Snap only children with includeInHandlerProfCrumb
          val acc = Seq.newBuilder[ProfiledEventCause]
          foreachChild(children) { child =>
            if (child.includeInHandlerProfCrumb) acc += child.snap(false)
          }
          acc.result()
        })
      )
    }

    // Return a snapshot of this profiled event cause data.
    def snap(includeChildren: Boolean): ProfiledEventCause = synchronized {
      // This is synchronized so that every ProfiledEventCause is an atomic snapshot. However children might change as
      // we are calculating, so the different events might not be entirely consistent with their parents and siblings.
      ProfiledEventCause(
        eventName = cause,
        profilingData = profilingMetadata,
        startTimeMs = eventStartTimeNs / NanosPerMillis,
        totalDurationMs = duration / NanosPerMillis,
        actionSelfTimeMs = actionSelfTimeNs / NanosPerMillis,
        // this shouldn't recurse too deep, otherwise we got other problems.
        childEvents = if (includeChildren) children.map(_.snap(includeChildren)) ++ externalProfiledChildren else Nil,
      )
    }

    override def newChild(cause: String, includeInHandlerProfCrumb: Boolean): ECProfilerLeaf = {
      val out = new LeafImpl(cause, includeInHandlerProfCrumb, depth + 1)
      synchronized { _children +:= out }
      out
    }

    override def addExternalProfiledChildren(children: Seq[ProfiledEventCause]): Unit =
      if (children.nonEmpty) {
        synchronized { _externalProfiledChildren ++= children }
      }

    // last update time if the event has been updated, or now
    private def duration: Long = {
      val end = eventEndTimeNs
      (if (end > 0L) end else OGTrace.nanoTime()) - eventStartTimeNs
    }

    private def children: List[LeafImpl] = _children

    private def externalProfiledChildren: List[ProfiledEventCause] = _externalProfiledChildren

    override def update(
        incrSelfTimeNs: Long = 0L,
        metadataToAdd: Map[String, String] = Map.empty,
        newGlobalTag: Option[String] = None
    ): Unit = synchronized {
      actionSelfTimeNs += incrSelfTimeNs
      profilingMetadata ++= metadataToAdd

      // Mark the whole root has having profiled children IFF the profiling meta is not empty or the global tag is not
      // empty.
      if ((!iAmRoot) && profilingMetadata.nonEmpty || newGlobalTag.nonEmpty) handler._hasProfiledChildren = true

      newGlobalTag match {
        case None =>
        case Some(newTag) =>
          handler.synchronized {
            handler._globalTag match {
              case Tagged(depth, _) if this.depth >= depth =>
              case _                                       => handler._globalTag = Tagged(depth, newTag)
            }
          }
      }
    }

    override def markCompleted(): Unit = synchronized {
      eventEndTimeNs = OGTrace.nanoTime()
    }

    // All the internal state is here. This is either immutable or updated under synchronization.
    override val eventStartTimeNs: Long = OGTrace.nanoTime()
    private var eventEndTimeNs: Long = _
    private var actionSelfTimeNs: Long = _
    private var profilingMetadata: Map[String, String] = Map.empty
    private var _children: List[LeafImpl] = Nil
    private var _externalProfiledChildren: List[ProfiledEventCause] = Nil

    private def iAmRoot = leaf eq this
  }
}

trait ECProfilerLeaf {

  /**
   * Produce a summary of profiling data for foreground children
   *
   *   1. Only this event and children event causes where includeInProfHandlerCrumbs is set to true are included
   *   1. All profiled children are included directly as children of the target profile.
   */
  def getSummaryProfilingData: ProfiledEventCause

  def newChild(cause: String, includeInHandlerProfCrumb: Boolean): ECProfilerLeaf

  def addExternalProfiledChildren(children: Seq[ProfiledEventCause]): Unit

  /**
   * Atomically update profiling information.
   *
   * incrSelfNs time is added to the self time of this task, the metadata added to existing metadata map and a
   * newGlobalTag is added if present
   */
  def update(
      incrSelfTimeNs: Long = 0L,
      metadataToAdd: Map[String, String] = Map.empty,
      newGlobalTag: Option[String] = None): Unit

  /**
   * Sets the eventEndTime to now.
   */
  def markCompleted(): Unit

  // Start time of this event.
  def eventStartTimeNs: Long
}
