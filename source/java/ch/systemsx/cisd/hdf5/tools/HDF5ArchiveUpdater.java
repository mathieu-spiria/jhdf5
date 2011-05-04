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
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import ch.systemsx.cisd.hdf5.HDF5FactoryProvider;
import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5OpaqueType;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.SyncMode;

/**
 * A class to create or update <code>h5ar</code> archives.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiveUpdater
{
    private static final String OPAQUE_TAG_FILE = "FILE";

    private static final int SIZEHINT_FACTOR = 5;

    private static final int MIN_GROUP_MEMBER_COUNT_TO_COMPUTE_SIZEHINT = 100;

    private final IHDF5Writer hdf5Writer;

    private final ArchivingStrategy strategy;

    private final boolean continueOnError;

    private final byte[] buffer;

    public HDF5ArchiveUpdater(File archiveFile, boolean noSync, FileFormat fileFormat,
            boolean continueOnError)
    {
        this(createHDF5Writer(archiveFile, fileFormat, noSync), new ArchivingStrategy(),
                continueOnError, new byte[HDF5Archiver.BUFFER_SIZE]);
    }

    static IHDF5Writer createHDF5Writer(File archiveFile, FileFormat fileFormat,
            boolean noSync)
    {
        final IHDF5WriterConfigurator config = HDF5FactoryProvider.get().configure(archiveFile);
        config.fileFormat(fileFormat);
        config.useUTF8CharacterEncoding();
        if (noSync == false)
        {
            config.syncMode(SyncMode.SYNC);
        }
        return config.writer();
    }

    public HDF5ArchiveUpdater(IHDF5Writer hdf5Writer, ArchivingStrategy strategy,
            boolean continueOnError, byte[] buffer)
    {
        this.hdf5Writer = hdf5Writer;
        this.continueOnError = continueOnError;
        this.strategy = strategy;
        this.buffer = buffer;
    }

    public void close()
    {
        hdf5Writer.close();
    }

    public HDF5ArchiveUpdater archiveAll(File path, boolean verbose) throws IllegalStateException
    {
        final File absolutePath = path.getAbsoluteFile();
        return archive(absolutePath.getParentFile(), absolutePath, verbose);
    }

    public HDF5ArchiveUpdater archive(File root, File path, boolean verbose)
            throws IllegalStateException
    {
        final File absoluteRoot = root.getAbsoluteFile();
        final File absolutePath = path.getAbsoluteFile();
        final boolean ok;
        int crc32 = 0;
        final Link linkOrNull = Link.tryCreate(absolutePath, true, continueOnError);
        if (linkOrNull != null && linkOrNull.isSymLink())
        {
            ok = archiveSymLink("", linkOrNull, absolutePath);
        } else if (absolutePath.isDirectory())
        {
            ok = archiveDirectory(absoluteRoot, absolutePath, verbose);
        } else if (absolutePath.isFile())
        {
            final Link pseudoLinkForChecksum = new Link();
            ok = archiveFile(absoluteRoot, absolutePath, pseudoLinkForChecksum, verbose);
            crc32 = pseudoLinkForChecksum.getCrc32();
        } else
        {
            ok = false;
            HDF5ArchiveOutputHelper.dealWithError(new ArchivingException(absolutePath,
                    new IOException("Path corresponds to neither a file nor a directory.")),
                    continueOnError);
        }
        if (ok)
        {
            updateIndicesOnThePath(absoluteRoot, absolutePath, crc32);
        }
        return this;
    }

    private void updateIndicesOnThePath(File root, File path, int crc32)
    {
        final String rootAbsolute = root.getAbsolutePath();
        File pathProcessing = path;
        int crc32Processing = crc32;
        while (true)
        {
            File dirProcessingOrNull = pathProcessing.getParentFile();
            String dirAbsolute =
                    (dirProcessingOrNull != null) ? dirProcessingOrNull.getAbsolutePath() : "";
            if (dirAbsolute.startsWith(rootAbsolute) == false)
            {
                break;
            }
            final String hdf5GroupPath = getRelativePath(rootAbsolute, dirAbsolute);
            final DirectoryIndex index =
                    new DirectoryIndex(hdf5Writer, hdf5GroupPath, continueOnError, false);
            final Link linkOrNull =
                    Link.tryCreate(pathProcessing, strategy.doStoreOwnerAndPermissions(),
                            continueOnError);
            if (linkOrNull != null)
            {
                linkOrNull.setCrc32(crc32Processing);
                crc32Processing = 0; // Directories don't have a checksum
                index.addToIndex(Collections.singletonList(linkOrNull));
                index.writeIndexToArchive();
            }
            pathProcessing = dirProcessingOrNull;
            dirProcessingOrNull = pathProcessing.getParentFile();
            dirAbsolute =
                    (dirProcessingOrNull != null) ? dirProcessingOrNull.getAbsolutePath() : "";
        }
    }

    private boolean archiveDirectory(File root, File dir, boolean verbose)
            throws ArchivingException
    {
        final File[] fileEntries = dir.listFiles();
        if (fileEntries == null)
        {
            HDF5ArchiveOutputHelper.dealWithError(new ArchivingException(dir, new IOException(
                    "Cannot read directory")), continueOnError);
            return false;
        }
        final String hdf5GroupPath = getRelativePath(root, dir);
        if (hdf5Writer.getFileFormat() != FileFormat.STRICTLY_1_8
                && fileEntries.length > MIN_GROUP_MEMBER_COUNT_TO_COMPUTE_SIZEHINT
                && "/.".equals(hdf5GroupPath) == false)
        {
            try
            {
                // Compute size hint and pre-create group in order to improve performance.
                int totalLength = computeSizeHint(fileEntries);
                hdf5Writer.createGroup(hdf5GroupPath, totalLength * SIZEHINT_FACTOR);
            } catch (HDF5Exception ex)
            {
                HDF5ArchiveOutputHelper.dealWithError(new ArchivingException(hdf5GroupPath, ex),
                        continueOnError);
            }
        }
        final List<Link> linkEntries =
                DirectoryIndex.convertFilesToLinks(fileEntries,
                        strategy.doStoreOwnerAndPermissions(), continueOnError);

        HDF5ArchiveOutputHelper.writeToConsole(hdf5GroupPath, verbose);
        final Iterator<Link> linkIt = linkEntries.iterator();
        for (int i = 0; i < fileEntries.length; ++i)
        {
            final File file = fileEntries[i];
            final Link linkOrNull = linkIt.next();
            if (linkOrNull == null)
            {
                linkIt.remove();
                continue;
            }
            final String absoluteEntry = file.getAbsolutePath();
            if (linkOrNull.isDirectory())
            {
                if (strategy.doExclude(absoluteEntry, true))
                {
                    linkIt.remove();
                    continue;
                }
                final boolean ok = archiveDirectory(root, file, verbose);
                if (ok == false)
                {
                    linkIt.remove();
                }
            } else
            {
                if (strategy.doExclude(absoluteEntry, false))
                {
                    linkIt.remove();
                    continue;
                }
                if (linkOrNull.isSymLink())
                {
                    final boolean ok = archiveSymLink(hdf5GroupPath, linkOrNull, file);
                    if (ok == false)
                    {
                        linkIt.remove();
                    }
                } else if (linkOrNull.isRegularFile())
                {
                    final boolean ok = archiveFile(root, file, linkOrNull, verbose);
                    if (ok == false)
                    {
                        linkIt.remove();
                    }
                } else
                {
                    HDF5ArchiveOutputHelper.dealWithError(
                            new ArchivingException(file, new IOException(
                                    "Path corresponds to neither a file nor a directory.")),
                            continueOnError);
                }
            }
        }

        final DirectoryIndex index =
                new DirectoryIndex(hdf5Writer, hdf5GroupPath, continueOnError, verbose);
        index.addToIndex(linkEntries);
        index.writeIndexToArchive();
        return true;
    }

    private boolean archiveSymLink(String hdf5GroupPath, Link link, File file)
    {
        final String linkTargetOrNull = Link.tryReadLinkTarget(file);
        if (linkTargetOrNull == null)
        {
            HDF5ArchiveOutputHelper.dealWithError(new ArchivingException(file, new IOException(
                    "Cannot read link target of symbolic link.")), continueOnError);
            return false;
        }
        try
        {
            hdf5Writer.createSoftLink(linkTargetOrNull, hdf5GroupPath + "/" + link.getLinkName());
        } catch (HDF5Exception ex)
        {
            HDF5ArchiveOutputHelper.dealWithError(
                    new ArchivingException(hdf5GroupPath + "/" + link.getLinkName(), ex),
                    continueOnError);
            return false;
        }
        return true;

    }

    private static int computeSizeHint(final File[] entries)
    {
        int totalLength = 0;
        for (File entry : entries)
        {
            totalLength += entry.getName().length();
        }
        return totalLength;
    }

    private boolean archiveFile(File root, File file, Link link, boolean verbose)
            throws ArchivingException
    {
        boolean ok = true;
        final String hdf5ObjectPath = getRelativePath(root, file);
        final HDF5GenericStorageFeatures compression = strategy.doCompress(hdf5ObjectPath);
        try
        {
            final long size = file.length();
            final int crc32 = copyToHDF5(file, hdf5ObjectPath, size, compression);
            link.setCrc32(crc32);
            HDF5ArchiveOutputHelper.writeToConsole(hdf5ObjectPath, verbose);
        } catch (IOException ex)
        {
            ok = false;
            HDF5ArchiveOutputHelper
                    .dealWithError(new ArchivingException(file, ex), continueOnError);
        } catch (HDF5Exception ex)
        {
            ok = false;
            HDF5ArchiveOutputHelper.dealWithError(new ArchivingException(hdf5ObjectPath, ex),
                    continueOnError);
        }
        return ok;
    }

    private static String getRelativePath(File root, File entry)
    {
        return getRelativePath(root.getAbsolutePath(), entry.getAbsolutePath());
    }

    private static String getRelativePath(String root, String entry)
    {
        final String path = entry.substring(root.length());
        if (path.length() == 0)
        {
            return "/";
        } else
        {
            return FilenameUtils.separatorsToUnix(path);
        }
    }

    private int copyToHDF5(File source, final String objectPath, final long size,
            final HDF5GenericStorageFeatures compression) throws IOException
    {
        final InputStream input = FileUtils.openInputStream(source);
        final int blockSize = (int) Math.min(size, buffer.length);
        final HDF5OpaqueType type;
        if (hdf5Writer.exists(objectPath, false))
        {
            type = hdf5Writer.tryGetOpaqueType(objectPath);
            if (type == null || OPAQUE_TAG_FILE.equals(type.getTag()) == false)
            {
                throw new HDF5JavaException("Object " + objectPath + " is not an opaque type '"
                        + OPAQUE_TAG_FILE + "'");
            }
        } else
        {
            type =
                    hdf5Writer.createOpaqueByteArray(objectPath, OPAQUE_TAG_FILE, size, blockSize,
                            compression);

        }
        final CRC32 crc32 = new CRC32();
        try
        {
            long count = 0;
            int n = 0;
            while (-1 != (n = input.read(buffer)))
            {
                hdf5Writer.writeOpaqueByteArrayBlockWithOffset(objectPath, type, buffer, n, count);
                count += n;
                crc32.update(buffer, 0, n);
            }
        } finally
        {
            IOUtils.closeQuietly(input);
        }
        return (int) crc32.getValue();
    }

}
