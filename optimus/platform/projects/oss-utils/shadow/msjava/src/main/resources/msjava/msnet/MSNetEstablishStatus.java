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
public class MSNetEstablishStatus {
    public static final int Unknown = 0;
    public static final int Failure = 1;
    public static final int Continue = 2;
    public static final int Complete = 3;
    public static final String STATE_STRINGS[] = new String[] { "Unknown", "Failure", "Continue", "Complete" };
    public static final MSNetEstablishStatus UNKNOWN = new MSNetEstablishStatus(Unknown);
    public static final MSNetEstablishStatus FAILURE = new MSNetEstablishStatus(Failure);
    public static final MSNetEstablishStatus CONTINUE = new MSNetEstablishStatus(Continue);
    public static final MSNetEstablishStatus COMPLETE = new MSNetEstablishStatus(Complete);
    private final int state;
    private final String msg;
    public MSNetEstablishStatus(int state, String msg) {
        this.state = state;
        this.msg = msg;
    }
    public MSNetEstablishStatus(int state) {
        this.state = state;
        msg = null;
    }
    public int getState() {
        return state;
    }
    public String toString() {
        String s;
        if (state < 0 || state > STATE_STRINGS.length) {
            s = "Invalid state";
        } else {
            s = STATE_STRINGS[state];
        }
        if (msg != null) {
            s += ": " + msg;
        }
        return s;
    }
    public boolean isUnknown() {
        return state == Unknown;
    }
    public boolean isFailure() {
        return state == Failure;
    }
    public boolean isContinue() {
        return state == Continue;
    }
    public boolean isComplete() {
        return state == Complete;
    }
    public static MSNetEstablishStatus failure(String msg) {
        return new MSNetEstablishStatus(Failure, msg);
    }
}
