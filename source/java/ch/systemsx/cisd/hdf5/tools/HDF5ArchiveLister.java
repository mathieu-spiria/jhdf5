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

package ch.systemsx.cisd.hdf5.tools;

import java.io.File;
import java.io.IOException;
import java.util.zip.CRC32;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import org.apache.commons.io.FilenameUtils;

import ch.systemsx.cisd.hdf5.IHDF5Reader;

/**
 * A lister for <code>h5ar</code> archives.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiveLister
{

    private final IHDF5Reader hdf5Reader;

    private final ArchivingStrategy strategy;

    private final IErrorStrategy errorStrategy;

    private byte[] buffer;

    public HDF5ArchiveLister(File archiveFile, IErrorStrategy errorStrategyOrNull)
    {
        this(HDF5ArchiveExtractor.createHDF5Reader(archiveFile), new ArchivingStrategy(),
                errorStrategyOrNull, new byte[HDF5Archiver.BUFFER_SIZE]);
    }

    public HDF5ArchiveLister(IHDF5Reader hdf5Reader, ArchivingStrategy strategy,
            IErrorStrategy errorStrategyOrNull, byte[] buffer)
    {
        this.hdf5Reader = hdf5Reader;
        if (errorStrategyOrNull == null)
        {
            this.errorStrategy = IErrorStrategy.DEFAULT_ERROR_STRATEGY;
        } else
        {
            this.errorStrategy = errorStrategyOrNull;
        }
        this.strategy = strategy;
        this.buffer = buffer;
    }

    public void close()
    {
        hdf5Reader.close();
    }

    public void list(String fileOrDir, String rootOrNull, boolean recursive,
            boolean suppressDirectoryEntries, boolean verbose, boolean numeric, Check check,
            IListEntryVisitor visitor)
    {
        final ListParameters params =
                new ListParameters().fileOrDirectoryInArchive(fileOrDir)
                        .directoryOnFileSystem(rootOrNull).strategy(strategy).recursive(recursive)
                        .suppressDirectoryEntries(suppressDirectoryEntries).numeric(numeric)
                        .verbose(verbose).check(check);
        params.check();
        final String objectPath = params.getFileOrDirectoryInArchive();
        final boolean isDirectory = hdf5Reader.isGroup(objectPath, false);
        if (params.getStrategy().doExclude(objectPath, isDirectory))
        {
            return;
        }
        if (isDirectory)
        {
            list(objectPath, visitor, params, new IdCache());
        } else
        {
            final String dir = FilenameUtils.getFullPathNoEndSeparator(objectPath);
            final Link linkOrNull =
                    new DirectoryIndex(hdf5Reader, "".equals(dir) ? "/" : dir, errorStrategy,
                            params.isVerbose()).tryGetLink(FilenameUtils.getName(objectPath));
            if (linkOrNull == null)
            {
                errorStrategy.dealWithError(new ListArchiveException(objectPath,
                        "Object not found in archive."));
                return;
            }
            try
            {
                process(objectPath, linkOrNull, visitor, params, new IdCache());
            } catch (IOException ex)
            {
                final File f = new File(objectPath);
                errorStrategy.dealWithError(new ListArchiveException(f, ex));
            } catch (HDF5Exception ex)
            {
                errorStrategy.dealWithError(new ListArchiveException(objectPath, ex));
            }
        }
    }

    /**
     * Provide the entries of <var>dir</var> to <var>visitor</var> recursively.
     */
    private void list(String dir, IListEntryVisitor visitor, ListParameters params, IdCache idCache)
    {
        if (hdf5Reader.exists(dir, false) == false)
        {
            errorStrategy.dealWithError(new ListArchiveException(dir,
                    "Directory not found in archive."));
            return;
        }
        final String dirPrefix = dir.endsWith("/") ? dir : (dir + "/");
        String path = "UNKNOWN";
        for (Link link : new DirectoryIndex(hdf5Reader, dir, errorStrategy, params.isVerbose()))
        {
            try
            {
                path = dirPrefix + link.getLinkName();
                if (params.getStrategy().doExclude(path, link.isDirectory()))
                {
                    continue;
                }
                if (link.isDirectory() == false || params.isSuppressDirectoryEntries() == false)
                {
                    process(path, link, visitor, params, idCache);
                }
                if (params.isRecursive() && link.isDirectory() && "/.".equals(path) == false)
                {
                    list(path, visitor, params, idCache);
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

    private void process(String path, Link link, IListEntryVisitor visitor, ListParameters params,
            IdCache idCache) throws IOException
    {
        final String errorLineOrNull = doCheck(path, link, params, idCache);
        if (errorLineOrNull == null)
        {
            visitor.visit(new ListEntry(path, link, idCache, params.isVerbose(), params.isNumeric()));
        } else
        {
            visitor.visit(new ListEntry(errorLineOrNull));
        }
    }

    private String doCheck(String path, Link link, ListParameters params, IdCache idCache)
            throws IOException
    {
        if (ListParameters.VERIFY_FS.contains(params.getCheck()))
        {
            return HDF5ArchiveLinkChecker.checkLink(link, path, params, idCache, buffer);
        } else if (Check.CHECK_CRC_ARCHIVE == params.getCheck())
        {
            return performArchiveCheck(path, link, params);
        }
        return null;
    }

    private String performArchiveCheck(String path, Link link, ListParameters params)
    {
        if (link.isRegularFile() == false)
        {
            return null;
        }
        final long size = hdf5Reader.getSize(path);
        if (link.getSize() != size)
        {
            return "Archive file " + path + " failed size test, expected: " + link.getSize()
                    + ", found: " + size;
        }
        if (link.getSize() != 0 && link.getCrc32() == 0)
        {
            return "Archive file " + path + ": cannot verify (missing CRC checksum).";
        }
        final int crc32 = calcCRC32Archive(path, link.getSize());
        if (link.getCrc32() != crc32)
        {
            return "Archive file " + path + " failed CRC checksum test, expected: "
                    + ListEntry.hashToString(link.getCrc32()) + ", found: "
                    + ListEntry.hashToString(crc32);
        }
        return null;
    }

    private int calcCRC32Archive(String objectPath, long size)
    {
        final CRC32 crc32Digest = new CRC32();
        long offset = 0;
        while (offset < size)
        {
            final int n =
                    hdf5Reader.readAsByteArrayToBlockWithOffset(objectPath, buffer, buffer.length,
                            offset, 0);
            offset += n;
            crc32Digest.update(buffer, 0, n);
        }
        return (int) crc32Digest.getValue();
    }

}
