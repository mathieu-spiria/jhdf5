/*
 * Copyright 2008 ETH Zuerich, CISD
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

package ch.systemsx.cisd.hdf5.h5ar;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;

/**
 * A class that represents a strategy for excluding files from archiving / extracting and for
 * compressing files in the archive.
 * 
 * @author Bernd Rinn
 */
public class ArchivingStrategy
{
    private List<Pattern> fileWhiteListOrNull;

    private List<Pattern> fileBlackListOrNull;

    private List<Pattern> dirWhiteListOrNull;

    private List<Pattern> dirBlackListOrNull;

    private List<Pattern> compressionWhiteListOrNull;

    private boolean compressAll;

    private boolean sealed;

    /**
     * The default strategy: include everything, compress nothing.
     */
    public static final ArchivingStrategy DEFAULT = new ArchivingStrategy();

    private List<Pattern> getOrCreateFileWhiteList()
    {
        if (fileWhiteListOrNull == null)
        {
            fileWhiteListOrNull = new ArrayList<Pattern>();
        }
        return fileWhiteListOrNull;
    }

    private List<Pattern> getOrCreateFileBlackList()
    {
        if (fileBlackListOrNull == null)
        {
            fileBlackListOrNull = new ArrayList<Pattern>();
        }
        return fileBlackListOrNull;
    }

    private List<Pattern> getOrCreateDirWhiteList()
    {
        if (dirWhiteListOrNull == null)
        {
            dirWhiteListOrNull = new ArrayList<Pattern>();
        }
        return dirWhiteListOrNull;
    }

    private List<Pattern> getOrCreateDirBlackList()
    {
        if (dirBlackListOrNull == null)
        {
            dirBlackListOrNull = new ArrayList<Pattern>();
        }
        return dirBlackListOrNull;
    }

    private List<Pattern> getOrCreateCompressionWhiteList()
    {
        if (compressionWhiteListOrNull == null)
        {
            compressionWhiteListOrNull = new ArrayList<Pattern>();
        }
        return compressionWhiteListOrNull;
    }

    private void checkSealed()
    {
        if (sealed)
        {
            throw new IllegalStateException("ArchivingStrategy is sealed.");
        }
    }

    /**
     * Add the given <var>pattern</var>to the whitelist of files to include in archiving.
     */
    public ArchivingStrategy addToFileWhiteList(Pattern pattern)
    {
        checkSealed();
        getOrCreateFileWhiteList().add(pattern);
        return this;
    }

    /**
     * Add the given <var>pattern</var>to the whitelist of files to include in archiving.
     */
    public ArchivingStrategy addToFileWhiteList(String pattern)
    {
        checkSealed();
        getOrCreateFileWhiteList().add(Pattern.compile(pattern));
        return this;
    }

    /**
     * Add the given <var>pattern</var>to the blacklist of files to exclude from archiving.
     */
    public ArchivingStrategy addToFileBlackList(Pattern pattern)
    {
        checkSealed();
        getOrCreateFileBlackList().add(pattern);
        return this;
    }

    /**
     * Add the given <var>pattern</var>to the blacklist of files to exclude from archiving.
     */
    public ArchivingStrategy addToFileBlackList(String pattern)
    {
        checkSealed();
        getOrCreateFileBlackList().add(Pattern.compile(pattern));
        return this;
    }

    /**
     * Add the given <var>pattern</var>to the whitelist of directories to include in archiving.
     */
    public ArchivingStrategy addToDirWhiteList(Pattern pattern)
    {
        checkSealed();
        getOrCreateDirWhiteList().add(pattern);
        return this;
    }

    /**
     * Add the given <var>pattern</var>to the whitelist of directories to include in archiving.
     */
    public ArchivingStrategy addToDirWhiteList(String pattern)
    {
        checkSealed();
        getOrCreateDirWhiteList().add(Pattern.compile(pattern));
        return this;
    }

    /**
     * Add the given <var>pattern</var>to the blacklist of directories to exclude from archiving.
     */
    public ArchivingStrategy addToDirBlackList(Pattern pattern)
    {
        checkSealed();
        getOrCreateDirBlackList().add(pattern);
        return this;
    }

    /**
     * Add the given <var>pattern</var>to the blacklist of directories to exclude from archiving.
     */
    public ArchivingStrategy addToDirBlackList(String pattern)
    {
        checkSealed();
        getOrCreateDirBlackList().add(Pattern.compile(pattern));
        return this;
    }

    /**
     * Add the given <var>pattern</var>to the whitelist of files to store compressed in archive.
     */
    public ArchivingStrategy addToCompressionWhiteList(Pattern pattern)
    {
        checkSealed();
        getOrCreateCompressionWhiteList().add(pattern);
        return this;
    }

    /**
     * Add the given <var>pattern</var>to the whitelist of files to store compressed in archive.
     */
    public ArchivingStrategy addToCompressionWhiteList(String pattern)
    {
        checkSealed();
        getOrCreateCompressionWhiteList().add(Pattern.compile(pattern));
        return this;
    }

    /**
     * Seal the strategy. After sealing, all attempts to modify the strategy will throw on
     * {@link IllegalStateException}.
     */
    public ArchivingStrategy seal()
    {
        this.sealed = true;
        return this;
    }

    /**
     * Returns <code>true</code> if this strategy is sealed.
     * 
     * @see #seal()
     */
    public boolean isSealed()
    {
        return sealed;
    }

    /**
     * Store all files compressed in archive.
     */
    public ArchivingStrategy compressAll()
    {
        checkSealed();
        this.compressAll = true;
        return this;
    }

    /**
     * Sets, whether all files should be stored compressed in archive (<code>true</code>) or not (
     * <code>false</code>).
     */
    public ArchivingStrategy compressAll(@SuppressWarnings("hiding")
    boolean compressAll)
    {
        checkSealed();
        this.compressAll = compressAll;
        return this;
    }

    /**
     * @deprecated Use {@link #compressAll(boolean)} instead.
     */
    @Deprecated
    public final void setCompressAll(boolean compressAll)
    {
        checkSealed();
        this.compressAll = compressAll;
    }

    boolean doStoreOwnerAndPermissions()
    {
        return true;
    }

    boolean doExclude(String path, boolean isDirectory)
    {
        if (isDirectory)
        {
            return match(dirBlackListOrNull, dirWhiteListOrNull, path) == false;
        } else
        {
            return match(fileBlackListOrNull, fileWhiteListOrNull, path) == false;
        }
    }

    HDF5GenericStorageFeatures getStorageFeatureForPath(String path)
    {
        return doCompress(path) ? HDF5GenericStorageFeatures.GENERIC_DEFLATE
                : HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION;
    }

    public boolean doCompress(String path)
    {
        if (compressAll)
        {
            return true;
        }
        if (compressionWhiteListOrNull == null)
        {
            return false;
        } else
        {
            return match(null, compressionWhiteListOrNull, path);
        }
    }

    public boolean isCompressAll()
    {
        return compressAll;
    }

    private static boolean match(Iterable<Pattern> blackListOrNull,
            Iterable<Pattern> whiteListOrNull, String path)
    {
        if (blackListOrNull != null)
        {
            for (Pattern p : blackListOrNull)
            {
                if (p.matcher(path).matches())
                {
                    return false;
                }
            }
        }
        if (whiteListOrNull == null)
        {
            return true;
        }
        for (Pattern p : whiteListOrNull)
        {
            if (p.matcher(path).matches())
            {
                return true;
            }
        }
        return false;
    }

}