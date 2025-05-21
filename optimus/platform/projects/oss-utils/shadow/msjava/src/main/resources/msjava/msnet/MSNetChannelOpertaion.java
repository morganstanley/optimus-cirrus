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
import java.nio.channels.SelectionKey;
public enum MSNetChannelOpertaion {
    READ(SelectionKey.OP_READ), WRITE(SelectionKey.OP_WRITE), ACCEPT(SelectionKey.OP_ACCEPT), CONNECT(
            SelectionKey.OP_CONNECT), ALL_OPS(SelectionKey.OP_ACCEPT | SelectionKey.OP_CONNECT | SelectionKey.OP_READ
            | SelectionKey.OP_WRITE);
    final int selectionKey;
    private MSNetChannelOpertaion(int selectionKey) {
        this.selectionKey = selectionKey;
    }
    static String getOpsString(int ops) {
        assert (ops & ~ALL_OPS.selectionKey) == 0;
        if (ops == 0) {
            return "NONE";
        }
        StringBuilder s = new StringBuilder();
        if ((ops & CONNECT.selectionKey) != 0)
            s.append("CONNECT ");
        if ((ops & ACCEPT.selectionKey) != 0)
            s.append("ACCEPT ");
        if ((ops & READ.selectionKey) != 0)
            s.append("READ ");
        if ((ops & WRITE.selectionKey) != 0)
            s.append("WRITE ");
        return s.toString();
    }
}
