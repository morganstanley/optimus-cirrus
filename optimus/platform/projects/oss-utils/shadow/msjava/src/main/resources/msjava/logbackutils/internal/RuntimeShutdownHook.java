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

package msjava.logbackutils.internal;
public class RuntimeShutdownHook {
    private final Runnable callback;
    private Thread shutdownHook;
    
    public RuntimeShutdownHook(Runnable callback) {
        if(callback == null) {
            throw new IllegalArgumentException("Callback can not be null."); 
        }
        this.callback = callback;
    }
    
    public synchronized void register() throws Throwable {
        shutdownHook = new Thread() {
            @Override
            public void run() {
                callback.run();
            }
        };
        try {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (SecurityException ex) {
            
            
            
            
            shutdownHook = null;
        } catch (Throwable t) {
            shutdownHook = null;
            throw t;
        }
    }
    
    
    public synchronized void unregister() throws Throwable {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (SecurityException ex) {
                
                shutdownHook = null;
            } catch (IllegalStateException ex) {
                
                
                shutdownHook = null;
            } catch (Throwable t) {
                shutdownHook = null;
                throw t;
            }
        }
    }
    
        
}
