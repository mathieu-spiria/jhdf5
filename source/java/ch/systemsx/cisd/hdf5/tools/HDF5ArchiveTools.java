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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import ch.systemsx.cisd.common.exceptions.WrappedIOException;
import ch.systemsx.cisd.common.utilities.OSUtilities;
import ch.systemsx.cisd.hdf5.HDF5LinkInformation;
import ch.systemsx.cisd.hdf5.HDF5Reader;
import ch.systemsx.cisd.hdf5.HDF5Writer;
import ch.systemsx.cisd.hdf5.HDF5OpaqueType;

/**
 * Tools for using HDF5 as archive format for directory with fast random access to particular files.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiveTools
{
    private static final String OPAQUE_TAG_FILE = "FILE";

    private static final int SIZEHINT_FACTOR = 10;

    private static final int MIN_GROUP_MEMBER_COUNT_TO_COMPUTE_SIZEHINT = 100;

    private final static int MB = 1024 * 1024;

    /**
     * Threshold for switching to block-wise I/O.
     */
    private final static int FILE_SIZE_THRESHOLD = 10 * MB;

    /**
     * Archives the <var>path</var>. It is expected that <var>path</var> is relative to
     * <var>root</var> which will be removed from the path before adding any file to the archive.
     */
    public static void archive(HDF5Writer writer, ArchivingStrategy strategy, File root, File path,
            boolean addEmptyDirectories, boolean continueOnError, boolean verbose)
            throws WrappedIOException
    {
        if (path.isDirectory())
        {
            archiveDirectory(writer, strategy, root, path, addEmptyDirectories, continueOnError,
                    verbose);
        } else
        {
            archiveFile(writer, strategy, root, path, continueOnError, verbose);
        }
    }

    private static void archiveDirectory(HDF5Writer writer, ArchivingStrategy strategy, File root,
            File dir, boolean addEmptyDirectories, boolean continueOnError, boolean verbose)
            throws ArchivingException
    {
        final File[] entries = dir.listFiles();
        if (entries == null)
        {
            dealWithError(new ArchivingException(dir, new IOException("Cannot read directory")),
                    continueOnError);
            return;
        }
        if (writer.isUseLatestFileFormat() == false
                && entries.length > MIN_GROUP_MEMBER_COUNT_TO_COMPUTE_SIZEHINT)
        {
            // Compute size hint to improve performance.
            int totalLength = 0;
            for (File entry : entries)
            {
                totalLength += entry.getName().length();
            }
            writer.createGroup(getRelativePath(root, dir), totalLength * SIZEHINT_FACTOR);
        }
        for (File entry : entries)
        {
            final String absoluteEntry = entry.getAbsolutePath();
            if (entry.isDirectory())
            {
                if (strategy.exclude(absoluteEntry, true))
                {
                    continue;
                }
                archiveDirectory(writer, strategy, root, entry, addEmptyDirectories,
                        continueOnError, verbose);
            } else
            {
                if (strategy.exclude(absoluteEntry, false))
                {
                    continue;
                }
                archiveFile(writer, strategy, root, entry, continueOnError, verbose);
            }
        }
        if (addEmptyDirectories && entries.length == 0)
        {
            final String hdf5ObjectPath = getRelativePath(root, dir);
            try
            {
                writer.createGroup(hdf5ObjectPath);
                list(hdf5ObjectPath, verbose);
            } catch (HDF5Exception ex)
            {
                dealWithError(new ArchivingException(hdf5ObjectPath, ex), continueOnError);
            }
        }
    }

    static void list(String hdf5ObjectPath, boolean verbose)
    {
        if (verbose)
        {
            System.out.println(hdf5ObjectPath);
        }
    }

    private static void archiveFile(HDF5Writer writer, ArchivingStrategy strategy, File root,
            File file, boolean continueOnError, boolean verbose) throws ArchivingException
    {
        final String hdf5ObjectPath = getRelativePath(root, file);
        final boolean compress = strategy.compress(hdf5ObjectPath);
        try
        {
            final long size = file.length();
            if (size > FILE_SIZE_THRESHOLD)
            {
                copyToHDF5Large(file, writer, hdf5ObjectPath, size, compress);
            } else
            {
                copyToHDF5Small(file, writer, hdf5ObjectPath, compress);
            }
            list(hdf5ObjectPath, verbose);
        } catch (IOException ex)
        {
            dealWithError(new ArchivingException(file, ex), continueOnError);
        } catch (HDF5Exception ex)
        {
            dealWithError(new ArchivingException(hdf5ObjectPath, ex), continueOnError);
        }
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
            return OSUtilities.isWindows() ? FilenameUtils.separatorsToUnix(path) : path;
        }
    }

    /**
     * Extracts the <var>path</var> from the HDF5 archive and stores it relative to <var>root</var>.
     */
    public static void extract(HDF5Reader reader, ArchivingStrategy strategy, File root,
            String path, boolean createEmptyDirectories, boolean continueOnError, boolean verbose)
            throws UnarchivingException
    {
        if (reader.exists(path) == false)
        {
            throw new UnarchivingException(path, "Object does not exist in archive.");
        }
        if (reader.isGroup(path))
        {
            extractDirectory(reader, strategy, root, path, createEmptyDirectories, continueOnError,
                    verbose);
        } else
        {
            extractFile(reader, strategy, root, path, continueOnError, verbose);
        }
    }

    private static void extractDirectory(HDF5Reader reader, ArchivingStrategy strategy, File root,
            String groupPath, boolean createEmptyDirectories, boolean continueOnError,
            boolean verbose) throws UnarchivingException
    {
        String objectPathOrNull = null;
        try
        {
            for (String path : reader.getGroupMemberPaths(groupPath))
            {
                objectPathOrNull = path;
                final HDF5LinkInformation infoOrNull = reader.getLinkInformation(path);
                if (infoOrNull == null)
                {
                    System.err.println("ERROR: Cannot get link information for path '" + path
                            + "'.");
                    continue;
                }
                if (infoOrNull.isGroup())
                {
                    if (strategy.exclude(path, true))
                    {
                        continue;
                    }
                    if (createEmptyDirectories)
                    {
                        (new File(root, path)).mkdir();
                        list(path, verbose);
                    }
                    extractDirectory(reader, strategy, root, path, createEmptyDirectories,
                            continueOnError, verbose);
                } else if (infoOrNull.isDataSet())
                {
                    extractFile(reader, strategy, root, path, continueOnError, verbose);
                } else
                {
                    System.err.println("Ignoring object '" + path + "' (type: "
                            + infoOrNull.getType() + ")");
                }
            }
        } catch (HDF5Exception ex)
        {
            dealWithError(new UnarchivingException(objectPathOrNull == null ? groupPath
                    : objectPathOrNull, ex), continueOnError);
        }
    }

    private static void extractFile(HDF5Reader reader, ArchivingStrategy strategy, File root,
            String hdf5ObjectPath, boolean continueOnError, boolean verbose)
            throws UnarchivingException
    {
        if (strategy.exclude(hdf5ObjectPath, false))
        {
            return;
        }
        final File file = new File(root, hdf5ObjectPath);
        file.getParentFile().mkdirs();
        try
        {
            final long size = reader.getDataSetInformation(hdf5ObjectPath).getSize();
            if (size > FILE_SIZE_THRESHOLD)
            {
                copyFromHDF5Large(reader, hdf5ObjectPath, size, file);
            } else
            {
                copyFromHDF5Small(reader, hdf5ObjectPath, file);
            }
            list(hdf5ObjectPath, verbose);
        } catch (IOException ex)
        {
            dealWithError(new UnarchivingException(file, ex), continueOnError);
        } catch (HDF5Exception ex)
        {
            dealWithError(new UnarchivingException(hdf5ObjectPath, ex), continueOnError);
        }
    }

    /**
     * Adds the entries of <var>dir</var> to <var>entries</var> recursively.
     */
    public static void addEntries(HDF5Reader reader, List<String> entries, String dir,
            boolean showSize, boolean addDirectories)
    {
        if (addDirectories)
        {
            entries.add(dir);
        }
        for (HDF5LinkInformation linkInfo : reader.getGroupMemberInformation(dir, false))
        {
            if (linkInfo.isGroup())
            {
                addEntries(reader, entries, linkInfo.getPath(), showSize, addDirectories);
            } else
            {
                if (showSize && linkInfo.isDataSet())
                {
                    entries.add(reader.getDataSetInformation(linkInfo.getPath()).getSize() + "\t"
                            + linkInfo.getPath());
                } else
                {
                    entries.add(linkInfo.getPath());
                }
            }
        }
    }

    static void dealWithError(final ArchiverException ex, boolean continueOnError)
            throws ArchivingException
    {
        if (continueOnError)
        {
            System.err.println(ex.getMessage());
        } else
        {
            if (ex.getCause() instanceof HDF5LibraryException)
            {
                System.err.println(((HDF5LibraryException) ex.getCause())
                        .getHDF5ErrorStackAsString());
            }
            throw ex;
        }
    }

    private static void copyToHDF5Small(File source, HDF5Writer writer, final String objectPath,
            final boolean compress) throws IOException
    {
        final byte[] data = FileUtils.readFileToByteArray(source);
        writer.writeOpaqueByteArray(objectPath, OPAQUE_TAG_FILE, data, compress);
    }

    private static void copyToHDF5Large(File source, final HDF5Writer writer,
            final String objectPath, final long size, final boolean compress) throws IOException
    {
        final InputStream input = FileUtils.openInputStream(source);
        final byte[] buffer = new byte[FILE_SIZE_THRESHOLD];
        final HDF5OpaqueType type =
                writer.createOpaqueByteArray(objectPath, OPAQUE_TAG_FILE, size,
                        FILE_SIZE_THRESHOLD, compress);
        try
        {
            long count = 0;
            int n = 0;
            while (-1 != (n = input.read(buffer)))
            {
                writer.writeOpaqueByteArrayBlockWithOffset(objectPath, type, buffer, n, count);
                count += n;
            }
        } finally
        {
            IOUtils.closeQuietly(input);
        }
    }

    private static void copyFromHDF5Small(HDF5Reader reader, String objectPath,
            final File destination) throws IOException
    {
        final byte[] data = reader.readAsByteArray(objectPath);
        FileUtils.writeByteArrayToFile(destination, data);
    }

    private static void copyFromHDF5Large(HDF5Reader reader, final String objectPath,
            final long size, File destination) throws IOException
    {
        final OutputStream output = FileUtils.openOutputStream(destination);
        try
        {
            final long blockCount =
                    (size / FILE_SIZE_THRESHOLD) + (size % FILE_SIZE_THRESHOLD != 0 ? 1 : 0);
            for (int i = 0; i < blockCount; ++i)
            {
                final byte[] buffer =
                        reader.readAsByteArrayBlock(objectPath, FILE_SIZE_THRESHOLD, i);
                output.write(buffer);
            }
        } finally
        {
            IOUtils.closeQuietly(output);
        }
    }
}
