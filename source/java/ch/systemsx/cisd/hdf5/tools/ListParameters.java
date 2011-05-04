/*
 * Copyright 201 ETH Zuerich, CISD
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.systemsx.cisd.hdf5.tools;

import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;


/**
 * A class to hold all parameters for the listing operation.
 * 
 * @author Bernd Rinn
 */
public final class ListParameters
{
    static final Set<Check> VERIFY_FS = EnumSet.of(Check.VERIFY_CRC_FS,
            Check.VERIFY_CRC_ATTR_FS);

    private String fileOrDirectoryInArchive;

    private String directoryOnFileSystem;

    private ArchivingStrategy strategy;

    private boolean recursive;

    private boolean verbose;

    private boolean numeric;

    private boolean suppressDirectoryEntries;

    Check check = Check.NO_CHECK;

    public ListParameters fileOrDirectoryInArchive(String newDirectoryInArchive)
    {
        this.fileOrDirectoryInArchive = newDirectoryInArchive;
        return this;
    }

    public ListParameters directoryOnFileSystem(String newDirectoryOnFileSystem)
    {
        this.directoryOnFileSystem = newDirectoryOnFileSystem;
        return this;
    }

    public ListParameters strategy(ArchivingStrategy newStrategy)
    {
        this.strategy = newStrategy;
        return this;
    }

    public ListParameters recursive(boolean newRecursive)
    {
        this.recursive = newRecursive;
        return this;
    }

    public ListParameters suppressDirectoryEntries(boolean newSuppressDirectories)
    {
        this.suppressDirectoryEntries = newSuppressDirectories;
        return this;
    }

    public ListParameters verbose(boolean newVerbose)
    {
        this.verbose = newVerbose;
        return this;
    }

    public ListParameters numeric(boolean newNumeric)
    {
        this.numeric = newNumeric;
        return this;
    }

    public ListParameters check(Check newCheck)
    {
        this.check = newCheck;
        return this;
    }

    public void check()
    {
        if (fileOrDirectoryInArchive == null)
        {
            throw new NullPointerException("fileOrDirectoryInArchive most not be null.");
        }
        fileOrDirectoryInArchive = FilenameUtils.separatorsToUnix(fileOrDirectoryInArchive);
        if (VERIFY_FS.contains(check) && directoryOnFileSystem == null)
        {
            throw new NullPointerException(
                    "fileOrDirectoryOnFileSystem most not be null when verifying.");
        }
        if (check == null)
        {
            throw new NullPointerException("check most not be null.");
        }
    }

    public String getFileOrDirectoryInArchive()
    {
        return fileOrDirectoryInArchive;
    }

    public String getFileOrDirectoryOnFileSystem()
    {
        return directoryOnFileSystem;
    }

    public ArchivingStrategy getStrategy()
    {
        return strategy;
    }

    public boolean isRecursive()
    {
        return recursive;
    }

    public boolean isSuppressDirectoryEntries()
    {
        return suppressDirectoryEntries;
    }

    public boolean isVerbose()
    {
        return verbose;
    }

    public boolean isNumeric()
    {
        return numeric;
    }

    public Check getCheck()
    {
        return check;
    }
}