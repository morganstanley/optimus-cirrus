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

package msjava.msnet;
public class MSNetLoopCyclePosition {
    
    public static final MSNetLoopCyclePosition PRE_CHANNEL_PROCESSING = new MSNetLoopCyclePosition(
            "PRE_CHANNEL_PROCESSING");
    
    public static final MSNetLoopCyclePosition POST_CHANNEL_PROCESSING = new MSNetLoopCyclePosition(
            "POST_CHANNEL_PROCESSING");
    protected final String name;
    public MSNetLoopCyclePosition(String name) {
        this.name = name;
    }
    public String toString() {
        return "MSNetLoopCyclePosition[" + name + "]";
    }
}
