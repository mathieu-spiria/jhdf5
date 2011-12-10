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
 * A traverser for <code>h5ar</code> archives.
 * 
 * @author Bernd Rinn
 */
class HDF5ArchiveTraverser
{
    private final IHDF5Reader hdf5Reader;

    private final DirectoryIndexProvider indexProvider;

    private final IErrorStrategy errorStrategy;

    private final IdCache idCache;

    public HDF5ArchiveTraverser(IHDF5Reader hdf5Reader, DirectoryIndexProvider indexProvider,
            IdCache idCache)
    {
        this.hdf5Reader = hdf5Reader;
        this.indexProvider = indexProvider;
        this.errorStrategy = indexProvider.getErrorStrategy();
        this.idCache = idCache;
    }

    public void process(String fileOrDir, boolean recursive, boolean readLinkTargets,
            IArchiveEntryProcessor processor)
    {
        final String normalizedPath = Utils.normalizePath(fileOrDir);
        final boolean isDirectory = hdf5Reader.isGroup(normalizedPath, false);

        final String parentPath = Utils.getParentPath(normalizedPath);
        LinkRecord link = null;
        if (parentPath.length() > 0)
        {
            link =
                    indexProvider.get(parentPath, readLinkTargets).tryGetLink(
                            normalizedPath.substring(parentPath.length() + 1));
            if (link == null)
            {
                errorStrategy.dealWithError(processor.createException(normalizedPath,
                        "Object not found in archive."));
                return;
            }
            try
            {
                if (processor.process(parentPath, normalizedPath, link, hdf5Reader, idCache,
                        errorStrategy) == false)
                {
                    return;
                }
            } catch (IOException ex)
            {
                final File f = new File(normalizedPath);
                errorStrategy.dealWithError(processor.createException(f, ex));
            } catch (HDF5Exception ex)
            {
                errorStrategy.dealWithError(processor.createException(normalizedPath, ex));
            }
        }
        if (isDirectory)
        {
            processDirectory(normalizedPath, recursive, readLinkTargets, processor);
            postProcessDirectory(parentPath, normalizedPath, link, processor);
        }
    }

    private void postProcessDirectory(final String parentPath, final String normalizedPath,
            LinkRecord linkOrNull, IArchiveEntryProcessor processor)
    {
        if (linkOrNull != null)
        {
            try
            {
                processor.postProcessDirectory(parentPath, normalizedPath, linkOrNull, hdf5Reader,
                        idCache, errorStrategy);
            } catch (IOException ex)
            {
                final File f = new File(normalizedPath);
                errorStrategy.dealWithError(processor.createException(f, ex));
            } catch (HDF5Exception ex)
            {
                errorStrategy.dealWithError(processor.createException(normalizedPath, ex));
            }
        }
    }

    /**
     * Provide the entries of <var>normalizedDir</var> to <var>processor</var>.
     */
    private void processDirectory(String normalizedDir, boolean recursive, boolean readLinkTargets,
            IArchiveEntryProcessor processor)
    {
        if (hdf5Reader.exists(normalizedDir, false) == false)
        {
            errorStrategy.dealWithError(processor.createException(normalizedDir,
                    "Directory not found in archive."));
            return;
        }
        for (LinkRecord link : indexProvider.get(normalizedDir, readLinkTargets))
        {
            final String path =
                    ("/".equals(normalizedDir) ? "/" : normalizedDir + "/") + link.getLinkName();
            try
            {
                if (processor
                        .process(normalizedDir, path, link, hdf5Reader, idCache, errorStrategy) == false)
                {
                    continue;
                }
                if (recursive && link.isDirectory())
                {
                    processDirectory(path, recursive, readLinkTargets, processor);
                    postProcessDirectory(normalizedDir, path, link, processor);
                }
            } catch (IOException ex)
            {
                final File f = new File(path);
                errorStrategy.dealWithError(processor.createException(f, ex));
            } catch (HDF5Exception ex)
            {
                errorStrategy.dealWithError(processor.createException(path, ex));
            }
        }
    }
}
