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
public class MSNetException extends Exception {
    private static final long serialVersionUID = -6230683631786596362L;
    
    public MSNetException(String message) {
        super(message);
    }
    
    public MSNetException(String message, Exception exception) {
        super(message, exception);
    }
    
    public MSNetException(Exception exception) {
        this(null, exception);
    }
    public String toString() {
        if (getCause() == null) {
            return super.toString();
        }
        return super.toString() + ": " + getCause();
    }
}
