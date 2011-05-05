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
import java.io.OutputStream;
import java.util.zip.CRC32;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import ch.systemsx.cisd.base.exceptions.IOExceptionUnchecked;
import ch.systemsx.cisd.base.unix.Unix;
import ch.systemsx.cisd.hdf5.HDF5FactoryProvider;
import ch.systemsx.cisd.hdf5.IHDF5Reader;

/**
 * An unarchiver for <code>h5ar</code> archives.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiveExtractor
{

    private static final int ROOT_UID = 0;

    private final IHDF5Reader hdf5Reader;

    private final IErrorStrategy errorStrategy;

    private final ArchivingStrategy strategy;

    private final byte[] buffer;

    public HDF5ArchiveExtractor(File archiveFile, IErrorStrategy errorStrategyOrNull)
    {
        this(createHDF5Reader(archiveFile), new ArchivingStrategy(), errorStrategyOrNull,
                new byte[HDF5Archiver.BUFFER_SIZE]);
    }

    static IHDF5Reader createHDF5Reader(File archiveFile)
    {
        return HDF5FactoryProvider.get().configureForReading(archiveFile)
                .useUTF8CharacterEncoding().reader();
    }

    public HDF5ArchiveExtractor(IHDF5Reader hdf5Reader, ArchivingStrategy strategy,
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

    public HDF5ArchiveExtractor extract(File root, String path, IPathVisitor pathVisitorOrNull)
            throws IllegalStateException
    {
        final String unixPath = FilenameUtils.separatorsToUnix(path);
        if (hdf5Reader.exists(unixPath, false) == false)
        {
            throw new UnarchivingException(unixPath, "Object does not exist in archive.");
        }
        final boolean isRoot = "/".equals(unixPath);
        final Link linkOrNull = isRoot ? null : tryGetLink(hdf5Reader, unixPath);
        final boolean isDir =
                (linkOrNull != null && linkOrNull.isDirectory())
                        || ((linkOrNull == null && (isRoot || hdf5Reader.isGroup(unixPath, false))));
        if (isDir)
        {
            extractDirectory(new GroupCache(), root, unixPath, linkOrNull, pathVisitorOrNull);
        } else
        {
            extractFile(new GroupCache(), root, unixPath, linkOrNull, pathVisitorOrNull);
        }
        return this;
    }

    /**
     * Returns the {@link Link} for <var>path</var> if that is stored in the directory index and
     * <code>null</code> otherwise.
     * <p>
     * <em>Note that a return value of <code>null</code> does not necessarily mean that <var>path</var>
     * is not in the archive!<em>
     */
    private Link tryGetLink(IHDF5Reader reader, String path)
    {
        final DirectoryIndex index =
                new DirectoryIndex(reader, FilenameUtils.separatorsToUnix(FilenameUtils
                        .getFullPathNoEndSeparator(path)), errorStrategy, true);
        return index.tryGetLink(FilenameUtils.getName(path));
    }

    private void extractDirectory(GroupCache groupCache, File root, String groupPath,
            Link dirLinkOrNull, IPathVisitor pathVisitorOrNull) throws UnarchivingException
    {
        String objectPathOrNull = null;
        try
        {
            final File groupFile = new File(root, groupPath);
            groupFile.mkdir();
            for (Link link : new DirectoryIndex(hdf5Reader, groupPath, errorStrategy, true))
            {
                objectPathOrNull =
                        (groupPath.endsWith("/") ? groupPath : (groupPath + "/"))
                                + link.getLinkName();
                if (link.isDirectory())
                {
                    if (strategy.doExclude(objectPathOrNull, true))
                    {
                        continue;
                    }
                    if (pathVisitorOrNull != null)
                    {
                        pathVisitorOrNull.visit(objectPathOrNull);
                    }
                    extractDirectory(groupCache, root, objectPathOrNull, link, pathVisitorOrNull);
                } else if (link.isRegularFile() || link.isSymLink())
                {
                    extractFile(groupCache, root, objectPathOrNull, link, pathVisitorOrNull);
                } else
                {
                    errorStrategy.dealWithError(new UnarchivingException(objectPathOrNull,
                            "Unexpected object type: "
                                    + tryGetObjectTypeDescriptionForErrorMessage(hdf5Reader,
                                            objectPathOrNull) + "."));
                }
            }
            restoreAttributes(groupFile, dirLinkOrNull, groupCache);
        } catch (HDF5Exception ex)
        {
            errorStrategy.dealWithError(new UnarchivingException(
                    objectPathOrNull == null ? groupPath : objectPathOrNull, ex));
        }
    }

    private static String tryGetObjectTypeDescriptionForErrorMessage(IHDF5Reader reader,
            String objectPath)
    {
        assert reader != null;
        assert objectPath != null;
        try
        {
            return reader.getLinkInformation(objectPath).getType().toString();
        } catch (HDF5LibraryException ex)
        {
            return "UNKNOWN";
        }
    }

    private void extractFile(GroupCache groupCache, File root, String hdf5ObjectPath,
            Link linkOrNull, IPathVisitor pathVisitorOrNull) throws UnarchivingException
    {
        if (strategy.doExclude(hdf5ObjectPath, false))
        {
            return;
        }
        final File file = new File(root, hdf5ObjectPath);
        file.getParentFile().mkdirs();
        final boolean isSymLink =
                (linkOrNull != null && linkOrNull.isSymLink())
                        || (linkOrNull == null && hdf5Reader.isSoftLink(hdf5ObjectPath));
        if (isSymLink)
        {
            if (Unix.isOperational())
            {
                try
                {
                    final String linkTargetOrNull =
                            (linkOrNull != null) ? linkOrNull.tryGetLinkTarget() : hdf5Reader
                                    .tryGetSymbolicLinkTarget(hdf5ObjectPath);
                    if (linkTargetOrNull == null)
                    {
                        errorStrategy.dealWithError(new UnarchivingException(hdf5ObjectPath,
                                "Cannot extract symlink as no link target stored."));
                    } else
                    {
                        Unix.createSymbolicLink(linkTargetOrNull, file.getAbsolutePath());
                        if (pathVisitorOrNull != null)
                        {
                            pathVisitorOrNull.visit(hdf5ObjectPath);
                        }
                    }
                } catch (IOExceptionUnchecked ex)
                {
                    errorStrategy.dealWithError(new UnarchivingException(file, ex));
                } catch (HDF5Exception ex)
                {
                    errorStrategy.dealWithError(new UnarchivingException(hdf5ObjectPath, ex));
                }
                return;
            } else
            {
                System.err.println("Warning: extracting symlink as regular file because"
                        + " Unix calls are not available on this system.");
            }
        }
        final int storedCrc32 = (linkOrNull != null) ? linkOrNull.getCrc32() : 0;
        try
        {
            final long size = hdf5Reader.getDataSetInformation(hdf5ObjectPath).getSize();
            final int crc32 = copyFromHDF5(hdf5ObjectPath, size, file);
            restoreAttributes(file, linkOrNull, groupCache);
            // storedCrc32 == 0 means: no checksum stored.
            final boolean checksumStored = (storedCrc32 != 0);
            final boolean checksumOK = (crc32 == storedCrc32);
            if (pathVisitorOrNull != null)
            {
                pathVisitorOrNull.visit(hdf5ObjectPath, checksumStored, checksumOK, crc32);
            }
            if (checksumStored && checksumOK == false)
            {
                errorStrategy.dealWithError(new UnarchivingException(hdf5ObjectPath,
                        "CRC checksum mismatch. Expected: " + ListEntry.hashToString(storedCrc32)
                                + ", found: " + ListEntry.hashToString(crc32)));
            }
        } catch (IOException ex)
        {
            errorStrategy.dealWithError(new UnarchivingException(file, ex));
        } catch (HDF5Exception ex)
        {
            errorStrategy.dealWithError(new UnarchivingException(hdf5ObjectPath, ex));
        }
    }

    private int copyFromHDF5(final String objectPath, final long size, File destination)
            throws IOException
    {
        final OutputStream output = FileUtils.openOutputStream(destination);
        final CRC32 crc32 = new CRC32();
        try
        {
            long offset = 0;
            while (offset < size)
            {
                final int n =
                        hdf5Reader.readAsByteArrayToBlockWithOffset(objectPath, buffer,
                                buffer.length, offset, 0);
                offset += n;
                output.write(buffer, 0, n);
                crc32.update(buffer, 0, n);
            }
            output.close(); // Make sure we don't silence exceptions on closing.
        } finally
        {
            IOUtils.closeQuietly(output);
        }
        return (int) crc32.getValue();
    }

    private void restoreAttributes(File file, Link linkInfoOrNull, GroupCache groupCache)
    {
        assert file != null;

        if (linkInfoOrNull != null)
        {
            if (linkInfoOrNull.hasLastModified())
            {
                file.setLastModified(linkInfoOrNull.getLastModified() * ListEntry.MILLIS_PER_SECOND);
            }
            if (linkInfoOrNull.hasUnixPermissions() && Unix.isOperational())
            {
                Unix.setAccessMode(file.getPath(), linkInfoOrNull.getPermissions());
                if (Unix.getUid() == ROOT_UID) // Are we root?
                {
                    Unix.setOwner(file.getPath(), linkInfoOrNull.getUid(), linkInfoOrNull.getGid());
                } else
                {
                    if (groupCache.isUserInGroup(linkInfoOrNull.getGid()))
                    {
                        Unix.setOwner(file.getPath(), Unix.getUid(), linkInfoOrNull.getGid());
                    }
                }
            }
        }
    }

}
