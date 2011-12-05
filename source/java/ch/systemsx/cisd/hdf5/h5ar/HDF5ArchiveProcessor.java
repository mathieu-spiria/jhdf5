/*
 * Copyright 2011 ETH Zuerich, CISD
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

import java.io.File;
import java.io.IOException;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import ch.systemsx.cisd.hdf5.IHDF5Reader;

/**
 * A processor for <code>h5ar</code> archives.
 * 
 * @author Bernd Rinn
 */
class HDF5ArchiveProcessor
{
    private final IHDF5Reader hdf5Reader;

    private final DirectoryIndexProvider indexProvider;

    private final IErrorStrategy errorStrategy;

    private final IdCache idCache;

    public HDF5ArchiveProcessor(IHDF5Reader hdf5Reader, DirectoryIndexProvider indexProvider)
    {
        this.hdf5Reader = hdf5Reader;
        this.indexProvider = indexProvider;
        this.errorStrategy = indexProvider.getErrorStrategy();
        this.idCache = new IdCache();
    }

    public void process(String fileOrDir, boolean recursive, boolean readLinkTargets,
            IArchiveEntryProcessor visitor)
    {
        final String normalizedPath = Utils.normalizePath(fileOrDir);
        final boolean isDirectory = hdf5Reader.isGroup(normalizedPath, false);

        final String parentPath = Utils.getParentPath(normalizedPath);
        if (parentPath.length() > 0)
        {
            final LinkRecord linkOrNull =
                    indexProvider.get(parentPath, readLinkTargets).tryGetLink(
                            normalizedPath.substring(parentPath.length() + 1));
            if (linkOrNull == null)
            {
                errorStrategy.dealWithError(new ListArchiveException(normalizedPath,
                        "Object not found in archive."));
                return;
            }
            try
            {
                if (visitor.process(parentPath, normalizedPath, linkOrNull, hdf5Reader, idCache) == false)
                {
                    return;
                }
            } catch (IOException ex)
            {
                final File f = new File(normalizedPath);
                errorStrategy.dealWithError(new ListArchiveException(f, ex));
            } catch (HDF5Exception ex)
            {
                errorStrategy.dealWithError(new ListArchiveException(normalizedPath, ex));
            }
        }
        if (isDirectory)
        {
            processDirectory(normalizedPath, recursive, readLinkTargets, visitor);
        }
    }

    /**
     * Provide the entries of <var>dir</var> to <var>visitor</var>.
     */
    private void processDirectory(String normalizedDir, boolean recursive, boolean readLinkTargets,
            IArchiveEntryProcessor visitor)
    {
        if (hdf5Reader.exists(normalizedDir, false) == false)
        {
            errorStrategy.dealWithError(new ListArchiveException(normalizedDir,
                    "Directory not found in archive."));
            return;
        }
        for (LinkRecord link : indexProvider.get(normalizedDir, readLinkTargets))
        {
            final String path =
                    (normalizedDir.endsWith("/") ? normalizedDir : normalizedDir + "/")
                            + link.getLinkName();
            try
            {
                if (visitor.process(normalizedDir, path, link, hdf5Reader, idCache) == false)
                {
                    continue;
                }
                if (recursive && link.isDirectory())
                {
                    processDirectory(path, recursive, readLinkTargets, visitor);
                }
            } catch (IOException ex)
            {
                final File f = new File(path);
                errorStrategy.dealWithError(new ListArchiveException(f, ex));
            } catch (HDF5Exception ex)
            {
                errorStrategy.dealWithError(new ListArchiveException(path, ex));
            }
        }
    }
}
