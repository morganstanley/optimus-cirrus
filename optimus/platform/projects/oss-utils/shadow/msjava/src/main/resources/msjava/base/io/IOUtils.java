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

package msjava.base.io;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import msjava.base.MSJavaBase;
public abstract class IOUtils {
    
    public static final int DEFAULT_BUFFER_SIZE = 2048;
    
    
    public static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ex) {
                MSJavaBase.LOGGER.warn("Ignoring unexpected exception: " + "Closing Closeable" + " :", ex);
            }
        }
    }
    
    
    public static File resolveAbsolutePathIfRelative(File file_, File baseDirIfRelative_)
            throws AbsolutePathResolutionException {
        if (!file_.isAbsolute()) {
            if (baseDirIfRelative_ == null) {
                throw new AbsolutePathResolutionException("File is a relative path, but no base directory given.",
                        file_, baseDirIfRelative_);
            }
            if (!baseDirIfRelative_.isAbsolute()) {
                throw new AbsolutePathResolutionException("Base directory is not an absolute path.", file_,
                        baseDirIfRelative_);
            }
            if (!baseDirIfRelative_.isDirectory()) {
                throw new AbsolutePathResolutionException("Base directory is not a directoy.", file_,
                        baseDirIfRelative_);
            }
            return new File(baseDirIfRelative_.getPath(), file_.getPath());
        }
        return file_;
    }
    
    public static int copy(Reader in_, Writer out_) throws IOException {
        return copy(in_, out_, new char[DEFAULT_BUFFER_SIZE]);
    }
    
    public static int copy(Reader in_, Writer out_, int bufferSize_) throws IOException {
        char[] buffer = new char[bufferSize_];
        int totalCharsCopied = 0;
        int charsRead = 0;
        while ((charsRead = in_.read(buffer)) != -1) {
            out_.write(buffer, 0, charsRead);
            totalCharsCopied += charsRead;
        }
        return totalCharsCopied;
    }
    
    public static int copy(Reader in_, Writer out_, char[] buffer_) throws IOException {
        int totalCharsCopied = 0;
        int charsRead = 0;
        while ((charsRead = in_.read(buffer_)) != -1) {
            out_.write(buffer_, 0, charsRead);
            totalCharsCopied += charsRead;
        }
        return totalCharsCopied;
    }
    
    public static int copy(InputStream in_, OutputStream out_) throws IOException {
        return copy(in_, out_, new byte[DEFAULT_BUFFER_SIZE]);
    }
    
    public static int copy(InputStream in_, OutputStream out_, int bufferSize_) throws IOException {
        return copy(in_, out_, new byte[bufferSize_]);
    }
    
    public static int copy(InputStream in_, OutputStream out_, byte[] buffer_) throws IOException {
        int totalBytesCopied = 0;
        int bytesRead = 0;
        while ((bytesRead = in_.read(buffer_)) != -1) {
            out_.write(buffer_, 0, bytesRead);
            totalBytesCopied += bytesRead;
        }
        return totalBytesCopied;
    }
    
    
    public static byte[] readBytes(InputStream in_) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
        copy(in_, result);
        result.close();
        return result.toByteArray();
    }
    
    public static byte[] readBytes(InputStream in_, int bufferSize_) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream(bufferSize_);
        copy(in_, result, bufferSize_);
        result.close();
        return result.toByteArray();
    }
    
    public static String readString(Reader in_) throws IOException {
        StringWriter result = new StringWriter(DEFAULT_BUFFER_SIZE);
        copy(in_, result);
        result.close();
        return result.toString();
    }
    
    public static String readString(Reader in_, int bufferSize_) throws IOException {
        StringWriter result = new StringWriter(bufferSize_);
        copy(in_, result, bufferSize_);
        result.close();
        return result.toString();
    }
    
    
    public static class AbsolutePathResolutionException extends RuntimeException {
        protected String _errorDescription;
        
        protected File _file;
        
        protected File _baseDirIfRelative;
        public AbsolutePathResolutionException(String errorDescription_, File file_, File baseDirIfRelative_) {
            super(errorDescription_ + " File: " + file_ + " BaseDir:" + baseDirIfRelative_);
            _errorDescription = errorDescription_;
            _file = file_;
            _baseDirIfRelative = baseDirIfRelative_;
        }
        public File getBaseDirIfRelative() {
            return _baseDirIfRelative;
        }
        public String getErrorDescription() {
            return _errorDescription;
        }
        public File getFile() {
            return _file;
        }
    }
}