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
package optimus.platform

import optimus.graph.NodeTask
import optimus.graph.PropertyNode
import optimus.graph.TweakTreeNode
import optimus.graph.tracking.ttracks.RefCounter

/* "Temporary" Overlay (because we throw it away, it's created on the fly for evaluation of the givenOverlay {} block) */
class TemporaryOverlayTweakableListener(
    val originalTweakableListener: TweakableListener,
    val overlayTweakableListener: TweakableListener,
    val forwardToListener: TweakableListener,
    trackingDepth: Int,
    overlayTrackingDepth: Int)
    extends TweakableListener {
  // our nodes can be invalidated by either of our parents (but not by ourselves as we have no invalidation mechanism)
  override val respondsToInvalidationsFrom: Set[TweakableListener] =
    originalTweakableListener.respondsToInvalidationsFrom ++ overlayTweakableListener.respondsToInvalidationsFrom

  override def isDisposed: Boolean = originalTweakableListener.isDisposed
  override val refQ: RefCounter[NodeTask] = overlayTweakableListener.refQ

  override def onTweakableNodeCompleted(node: PropertyNode[_]): Unit = {
    forwardToListener.onTweakableNodeCompleted(node)
    overlayTweakableListener.onTweakableNodeCompleted(node)
  }
  override def onTweakableNodeUsedBy(key: PropertyNode[_], node: NodeTask): Unit = {
    forwardToListener.onTweakableNodeUsedBy(key, node)
    overlayTweakableListener.onTweakableNodeUsedBy(key, node)
  }
  override def onTweakableNodeHashUsedBy(key: NodeHash, node: NodeTask): Unit = {
    forwardToListener.onTweakableNodeHashUsedBy(key, node)
    overlayTweakableListener.onTweakableNodeHashUsedBy(key, node)
  }
  override def onTweakUsedBy(ttn: TweakTreeNode, node: NodeTask): Unit = {
    if (ttn.trackingDepth <= trackingDepth) {
      if (ttn.trackingDepth == trackingDepth) { //  .. ttn.dbgTweakSS._tweakableListener eq this
        // if the tweak was found in us (or one of our descendant basic scenario stacks which report to us), it is
        // masking any tweaks in the original parent, so we only need to notify the overlay.
        // need to adjust the depth to the overlay's
        overlayTweakableListener.onTweakUsedBy(ttn.withDepth(ttn.tweak, overlayTrackingDepth), node)
      } else {
        // the tweak was found in our ancestors so we definitely need to tell our original parent
        forwardToListener.onTweakUsedBy(ttn, node)
        // we also need to tell the overlay because if it gets a tweak to such tweakable, it will start masking
        // the one from the parent.
        overlayTweakableListener.onTweakableNodeUsedBy(ttn.key, node)
      }
    }
    // else ignoring ttn.nested [SEE_ON_TWEAK_USED]
  }
  override def reportTweakableUseBy(node: NodeTask): Unit = {
    forwardToListener.reportTweakableUseBy(node)
    overlayTweakableListener.reportTweakableUseBy(node)
  }

  override def toString: String =
    s"TemporaryOverlayTweakableListener($originalTweakableListener overlayed with $overlayTweakableListener)"
}
