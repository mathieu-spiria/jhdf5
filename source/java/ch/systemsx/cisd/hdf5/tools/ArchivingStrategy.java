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

package ch.systemsx.cisd.hdf5.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

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

    public final void setCompressAll(boolean compressAll)
    {
        this.compressAll = compressAll;
    }

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
    
    public ArchivingStrategy addToFileWhiteList(Pattern pattern)
    {
        getOrCreateFileWhiteList().add(pattern);
        return this;
    }

    public ArchivingStrategy addToFileWhiteList(String pattern)
    {
        getOrCreateFileWhiteList().add(Pattern.compile(pattern));
        return this;
    }

    public ArchivingStrategy addToFileBlackList(Pattern pattern)
    {
        getOrCreateFileBlackList().add(pattern);
        return this;
    }

    public ArchivingStrategy addToFileBlackList(String pattern)
    {
        getOrCreateFileBlackList().add(Pattern.compile(pattern));
        return this;
    }

    public ArchivingStrategy addToDirWhiteList(Pattern pattern)
    {
        getOrCreateDirWhiteList().add(pattern);
        return this;
    }

    public ArchivingStrategy addToDirWhiteList(String pattern)
    {
        getOrCreateDirWhiteList().add(Pattern.compile(pattern));
        return this;
    }

    public ArchivingStrategy addToDirBlackList(Pattern pattern)
    {
        getOrCreateDirBlackList().add(pattern);
        return this;
    }

    public ArchivingStrategy addToDirBlackList(String pattern)
    {
        getOrCreateDirBlackList().add(Pattern.compile(pattern));
        return this;
    }

    public ArchivingStrategy addToCompressionWhiteList(Pattern pattern)
    {
        getOrCreateCompressionWhiteList().add(pattern);
        return this;
    }

    public ArchivingStrategy addToCompressionWhiteList(String pattern)
    {
        getOrCreateCompressionWhiteList().add(Pattern.compile(pattern));
        return this;
    }

    public boolean exclude(String path, boolean isDirectory)
    {
        if (isDirectory)
        {
            return match(dirBlackListOrNull, dirWhiteListOrNull, path) == false;
        } else
        {
            return match(fileBlackListOrNull, fileWhiteListOrNull, path) == false;
        }
    }

    public boolean compress(String path)
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