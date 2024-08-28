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
package optimus.graph.tracking.ttracks;

import optimus.graph.NodeTask;

/** adds a utility method for traversing TTrackRefs to a MutableTTrackStack */
abstract class TTrackRefTraverser extends MutableTTrackStack {
  protected void visitRef(NodeTask task, TTrackRef tref) {}

  protected void visitRef(TTrackRef tref) {}

  /**
   * subclasses must call this method if they want to be called back for ref and all of its children
   */
  protected final void visitAllRefs(TTrackRef ref) {
    while (ref != null) {
      NodeTask tsk = ref.get();
      if (tsk != null) visitRef(tsk, ref);
      else visitRef(ref);
      ref = ref.nref;
    }
  }
}
