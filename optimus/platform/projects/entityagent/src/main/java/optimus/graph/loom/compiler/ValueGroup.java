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
package optimus.graph.loom.compiler;

import java.util.ArrayList;

/**
 * Keeps track of potentially linked values (see aliasing).
 *
 * <pre>
 * mo2 = foo(mo1)
 * mo3 = goo(mo1, mo2)
 * </pre>
 */
class ValueGroup extends ArrayList<ValueGroup> {

  // Some Ops like invokedynamic have types that are technically immutable but if the captured
  // arguments are mutable, we can't trust this type and will start assuming it's mutable
  public boolean mutable;
  Op lastUpdate;

  Op creatorOp;

  public ValueGroup(Op lastUpdate) {
    this.lastUpdate = lastUpdate;
    this.creatorOp = lastUpdate;
  }

  static void link(Op a, Op b) {
    a.valueGroup.add(b.valueGroup);
    b.valueGroup.add(a.valueGroup);
  }

  void setLastUpdate(Op updateOp) {
    if (lastUpdate == updateOp) return;

    lastUpdate = updateOp;
    for (var next : this) next.setLastUpdate(updateOp);
  }
}
