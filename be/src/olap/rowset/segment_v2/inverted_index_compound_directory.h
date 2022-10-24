// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <CLucene.h>

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "io/fs/file_system.h"

namespace doris {

namespace segment_v2 {

class DorisCompoundFileWriter : LUCENE_BASE {
public:
    DorisCompoundFileWriter(CL_NS(store)::Directory* dir);
    ~DorisCompoundFileWriter() override = default;
    /** Returns the directory of the compound file. */
    CL_NS(store)::Directory* getDirectory();
    void writeCompoundFile();
    void copyFile(const char* fileName, lucene::store::IndexOutput* output, uint8_t* buffer,
                  int64_t bufferLength);

private:
    CL_NS(store)::Directory* directory;
};

class CLUCENE_EXPORT DorisCompoundDirectory : public lucene::store::Directory {
    /**
    * implementation of {@link lucene::store::Directory} as a directory of files backed by doris fs interface
    * <p>If the system property 'disableLuceneLocks' has the String value of
    * "true", lock creation will be disabled.
    *
    * @see Directory
    */
private:
    class FSIndexOutput;
    class FSIndexInput;
    friend class DorisCompoundDirectory::FSIndexOutput;
    friend class DorisCompoundDirectory::FSIndexInput;

    int filemode;

protected:
    DorisCompoundDirectory();
    virtual void init(io::FileSystem* fs, const char* path,
                      lucene::store::LockFactory* lockFactory = NULL);
    void priv_getFN(char* buffer, const char* name) const;

private:
    io::FileSystem* fs;
    std::string directory;
    int refCount;
    void create();

    static const char* LOCK_DIR;
    static const char* getLockDir();
    char* getLockPrefix() const;
    static bool disableLocks;

    bool useMMap;
    bool useCompoundFileWriter {false};

protected:
    /// Removes an existing file in the directory.
    bool doDeleteFile(const char* name) override;

public:
    io::FileSystem* getFileSystem() { return fs; }
    ///Destructor - only call this if you are sure the directory
    ///is not being used anymore. Otherwise use the ref-counting
    ///facilities of _CLDECDELETE
    ~DorisCompoundDirectory() override;

    /// Get a list of strings, one for each file in the directory.
    bool list(std::vector<std::string>* names) const override;

    /// Returns true iff a file with the given name exists.
    bool fileExists(const char* name) const override;

    /// Returns the text name of the directory
    const char* getDirName() const; ///<returns reference

    /**
    * Deprecated, see getDirectory(file, lockFactory)
    * Use IndexWriter's create flag, instead, to
    * create a new index.
    */
    static _CL_DEPRECATED(getDirectory(fs, file, lockFactory)) DorisCompoundDirectory* getDirectory(
            io::FileSystem* fs, const char* file, const bool create,
            lucene::store::LockFactory* lockFactory);

    /**
    Returns the directory instance for the named location.

    Do not delete this instance, only use close, otherwise other instances
    will lose this instance.

    <p>Directories are cached, so that, for a given canonical path, the same
    FSDirectory instance will always be returned.  This permits
    synchronization on directories.

    @param file the path to the directory.
    @param create if true, create, or erase any existing contents.
    @return the DorisFSDirectory for the named file.
    */
    static DorisCompoundDirectory* getDirectory(io::FileSystem* fs, const char* file,
                                                lucene::store::LockFactory* lockFactory = NULL);

    static DorisCompoundDirectory* getDirectory(io::FileSystem* fs, const char* file,
                                                bool useCompoundFileWriter);

    /// Returns the time the named file was last modified.
    int64_t fileModified(const char* name) const override;

    //static
    /// Returns the time the named file was last modified.
    static int64_t fileModified(const char* dir, const char* name);

    //static
    /// Returns the length in bytes of a file in the directory.
    int64_t fileLength(const char* name) const override;

    /// Returns a stream reading an existing file.
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                   int32_t bufferSize = -1) override;

    /// Renames an existing file in the directory.
    void renameFile(const char* from, const char* to) override;

    /** Set the modified time of an existing file to now. */
    void touchFile(const char* name) override;

    /// Creates a new, empty file in the directory with the given name.
    ///	Returns a stream writing this file.
    lucene::store::IndexOutput* createOutput(const char* name) override;

    ///Decrease the ref-count to the directory by one. If
    ///the object is no longer needed, then the object is
    ///removed from the directory pool.
    void close() override;

    /**
    * If MMap is available, this can disable use of
	  * mmap reading.
	  */
    void setUseMMap(bool value);
    /**
    * Gets whether the directory is using MMap for inputstreams.
	  */
    bool getUseMMap() const;

    std::string toString() const override;

    static const char* getClassName();
    const char* getObjectName() const override;

    /**
	  * Set whether Lucene's use of lock files is disabled. By default,
	  * lock files are enabled. They should only be disabled if the index
	  * is on a read-only medium like a CD-ROM.
	  */
    static void setDisableLocks(bool doDisableLocks);

    /**
	  * Returns whether Lucene's use of lock files is disabled.
	  * @return true if locks are disabled, false if locks are enabled.
	  */
    static bool getDisableLocks();

    /**
    * Sets the file mode for new files. This is passed to new output streams
    * and to the lock factory. The mode should be a valid octal file mode for
    * the 3rd parameter of the file open function (such as 0644)
    *
    * Tip: _tcstoi64(_T("644"), NULL, 8) is also a valid way of
    * creating a file mode
    */
    void setFileMode(int mode);

    /**
    * Gets the file mode for new files
    */
    int getFileMode();

    // Removes current directory
    bool deleteDirectory();
};

} // namespace segment_v2
} // namespace doris