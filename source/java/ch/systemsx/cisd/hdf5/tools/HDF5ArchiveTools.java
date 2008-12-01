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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import ch.systemsx.cisd.common.exceptions.WrappedIOException;
import ch.systemsx.cisd.common.os.FileLinkType;
import ch.systemsx.cisd.common.utilities.OSUtilities;
import ch.systemsx.cisd.hdf5.HDF5LinkInformation;
import ch.systemsx.cisd.hdf5.HDF5ObjectType;
import ch.systemsx.cisd.hdf5.HDF5OpaqueType;
import ch.systemsx.cisd.hdf5.HDF5Reader;
import ch.systemsx.cisd.hdf5.HDF5Writer;

/**
 * Tools for using HDF5 as archive format for directory with fast random access to particular files.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiveTools
{
    private static final String OPAQUE_TAG_FILE = "FILE";

    private static final String OPAQUE_TAG_INDEX = "INDEX";

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
            boolean continueOnError, boolean verbose) throws WrappedIOException
    {
        final LinkInfo[] info = new LinkInfo[1];
        info[0] = LinkInfo.get(path, strategy.doStoreOwnerAndPermissions());
        updateIndex(writer, "/", info);
        if (path.isDirectory())
        {
            archiveDirectory(writer, strategy, root, path, continueOnError, verbose);
        } else
        {
            archiveFile(writer, strategy, root, path, continueOnError, verbose);
        }
    }

    private static void archiveDirectory(HDF5Writer writer, ArchivingStrategy strategy, File root,
            File dir, boolean continueOnError, boolean verbose) throws ArchivingException
    {
        final File[] entries = dir.listFiles();
        if (entries == null)
        {
            dealWithError(new ArchivingException(dir, new IOException("Cannot read directory")),
                    continueOnError);
            return;
        }
        final String hdf5GroupPath = getRelativePath(root, dir);
        if (writer.isUseLatestFileFormat() == false
                && entries.length > MIN_GROUP_MEMBER_COUNT_TO_COMPUTE_SIZEHINT)
        {
            // Compute size hint and pre-create group in order to improve performance.
            int totalLength = computeSizeHint(entries);
            writer.createGroup(hdf5GroupPath, totalLength * SIZEHINT_FACTOR);
        }
        final LinkInfo[] linkInfos = getLinkInfos(entries, strategy.doStoreOwnerAndPermissions());
        try
        {
            updateIndex(writer, hdf5GroupPath, linkInfos);
        } catch (HDF5Exception ex)
        {
            dealWithError(new ArchivingException(hdf5GroupPath, ex), continueOnError);
        }

        list(hdf5GroupPath, verbose);
        for (int i = 0; i < entries.length; ++i)
        {
            final File file = entries[i];
            final LinkInfo info = linkInfos[i];
            final String absoluteEntry = file.getAbsolutePath();
            if (info.isDirectory())
            {
                if (strategy.doExclude(absoluteEntry, true))
                {
                    continue;
                }
                archiveDirectory(writer, strategy, root, file, continueOnError, verbose);
            } else
            {
                if (strategy.doExclude(absoluteEntry, false))
                {
                    continue;
                }
                if (info.isSymLink())
                {
                    final String linkTargetOrNull = LinkInfo.tryReadLinkTarget(file);
                    if (linkTargetOrNull == null)
                    {
                        dealWithError(new ArchivingException(file, new IOException(
                                "Cannot read link target of symbolic link.")), continueOnError);
                    }
                    writer.createSoftLink(linkTargetOrNull, hdf5GroupPath + "/"
                            + info.getLinkName());
                } else
                {
                    archiveFile(writer, strategy, root, file, continueOnError, verbose);
                }
            }
        }
    }

    private static String getIndexDataSetName(final String hdf5ObjectPath)
    {
        return hdf5ObjectPath + "/__INDEX__";
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

    private static LinkInfo[] tryReadIndex(final HDF5Writer writer, final String hdf5GroupPath)
    {
        final String indexDataSetName = getIndexDataSetName(hdf5GroupPath);
        if (writer.exists(indexDataSetName))
        {
            return LinkInfo.fromStorageForm(writer.readAsByteArray(indexDataSetName));
        } else
        {
            return null;
        }
    }

    private static Map<String, LinkInfo> tryGetIndexMap(final HDF5Writer writer,
            final String hdf5GroupPath)
    {
        LinkInfo[] infos = tryReadIndex(writer, hdf5GroupPath);
        if (infos == null)
        {
            if (writer.isGroup(hdf5GroupPath))
            {
                final List<HDF5LinkInformation> hdf5LinkInfos =
                        writer.getGroupMemberInformation(hdf5GroupPath, false);
                infos = new LinkInfo[hdf5LinkInfos.size()];
                for (int i = 0; i < hdf5LinkInfos.size(); ++i)
                {
                    final HDF5LinkInformation linfo = hdf5LinkInfos.get(i);
                    final long size = writer.getDataSetInformation(linfo.getPath()).getSize();
                    final FileLinkType type = translateType(linfo.getType());
                    infos[i] = new LinkInfo(linfo.getName(), type, size, 0L);
                }
            } else
            {
                return null;
            }
        }
        final HashMap<String, LinkInfo> map = new HashMap<String, LinkInfo>(infos.length);
        for (LinkInfo info : infos)
        {
            map.put(info.getLinkName(), info);
        }
        return map;
    }

    private static FileLinkType translateType(final HDF5ObjectType hdf5Type)
    {
        switch (hdf5Type)
        {
            case DATASET:
                return FileLinkType.REGULAR_FILE;
            case GROUP:
                return FileLinkType.DIRECTORY;
            case SOFT_LINK:
                return FileLinkType.SYMLINK;
            default:
                return FileLinkType.OTHER;
        }
    }

    private static LinkInfo[] getLinkInfos(final File[] entries, boolean storeOwnerAndPermissions)
    {
        final LinkInfo[] infos = new LinkInfo[entries.length];
        for (int i = 0; i < entries.length; ++i)
        {
            infos[i] = LinkInfo.get(entries[i], storeOwnerAndPermissions);
        }
        return infos;
    }

    private static void writeIndexMap(final HDF5Writer writer, final String hdf5GroupPath,
            final Map<String, LinkInfo> indexMap)
    {
        final LinkInfo[] infos = indexMap.values().toArray(new LinkInfo[indexMap.size()]);
        writeIndex(writer, hdf5GroupPath, infos);
    }

    private static void writeIndex(final HDF5Writer writer, final String hdf5GroupPath,
            final LinkInfo[] infos)
    {
        final String indexDataSetName = getIndexDataSetName(hdf5GroupPath);
        final LinkInfo[] sortedInfos = new LinkInfo[infos.length];
        System.arraycopy(infos, 0, sortedInfos, 0, infos.length);
        Arrays.sort(sortedInfos);
        writer.writeOpaqueByteArray(indexDataSetName, OPAQUE_TAG_INDEX, LinkInfo
                .toStorageForm(sortedInfos));
    }

    private static void updateIndex(final HDF5Writer writer, final String hdf5GroupPath,
            final LinkInfo[] newEntries)
    {
        final Map<String, LinkInfo> indexMapOrNull = tryGetIndexMap(writer, hdf5GroupPath);
        if (indexMapOrNull == null)
        {
            writeIndex(writer, hdf5GroupPath, newEntries);
        } else
        {
            for (LinkInfo info : newEntries)
            {
                indexMapOrNull.put(info.getLinkName(), info);
            }
            writeIndexMap(writer, hdf5GroupPath, indexMapOrNull);
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
        final boolean compress = strategy.doCompress(hdf5ObjectPath);
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
            String path, boolean continueOnError, boolean verbose) throws UnarchivingException
    {
        if (reader.exists(path) == false)
        {
            throw new UnarchivingException(path, "Object does not exist in archive.");
        }
        if (reader.isGroup(path))
        {
            extractDirectory(reader, strategy, root, path, continueOnError, verbose);
        } else
        {
            extractFile(reader, strategy, root, path, continueOnError, verbose);
        }
    }

    /**
     * Deletes the <var>path</var> from the HDF5 archive.
     */
    public static void delete(HDF5Writer writer, List<String> paths, boolean continueOnError,
            boolean verbose) throws UnarchivingException
    {
        Map<String, LinkInfo> indexMapOrNull = null;
        String lastGroup = null;
        for (String path : paths)
        {
            int groupDelimIndex = path.lastIndexOf('/');
            final String group = (groupDelimIndex < 2) ? "/" : path.substring(0, groupDelimIndex);
            if (group.equals(lastGroup) == false)
            {
                if (indexMapOrNull != null)
                {
                    writeIndexMap(writer, lastGroup, indexMapOrNull);
                }
                indexMapOrNull = tryGetIndexMap(writer, group);
            }
            try
            {
                writer.delete(path);
                if (indexMapOrNull != null)
                {
                    final String name = path.substring(groupDelimIndex + 1);
                    indexMapOrNull.remove(name);
                }
                list(path, verbose);
            } catch (HDF5Exception ex)
            {
                HDF5ArchiveTools.dealWithError(new DeleteFromArchiveException(path, ex),
                        continueOnError);
            }
        }
        if (indexMapOrNull != null)
        {
            writeIndexMap(writer, lastGroup, indexMapOrNull);
        }
    }

    private static void extractDirectory(HDF5Reader reader, ArchivingStrategy strategy, File root,
            String groupPath, boolean continueOnError, boolean verbose) throws UnarchivingException
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
                    if (strategy.doExclude(path, true))
                    {
                        continue;
                    }
                    (new File(root, path)).mkdir();
                    list(path, verbose);
                    extractDirectory(reader, strategy, root, path, continueOnError, verbose);
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
        if (strategy.doExclude(hdf5ObjectPath, false))
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
            boolean recursive, boolean verbose, boolean continueOnError)
    {
        if (reader.exists(dir) == false)
        {
            HDF5ArchiveTools.dealWithError(new ListArchiveException(dir, new HDF5JavaException(
                    "Directory not found in archive.")), continueOnError);
            return;
        }
        final String dirPrefix = dir.endsWith("/") ? dir : (dir + "/");
        final String indexDataSetName = getIndexDataSetName(dir);
        if (reader.exists(indexDataSetName))
        {
            final LinkInfo[] infos =
                    LinkInfo.fromStorageForm(reader.readAsByteArray(indexDataSetName));
            for (LinkInfo info : infos)
            {
                final String path = dirPrefix + info.getLinkName();
                if (verbose)
                {
                    if (info.isSymLink())
                    {
                        entries
                                .add(String
                                        .format(
                                                "%o\t%d/%d\t%10d\t%5$tY-%5$tm-%5$td %5$tH:%5$tM:%5$tS\t%6$s -> %7$s",
                                                info.getPermissions(), info.getUid(),
                                                info.getGid(), info.getSize(), info
                                                        .getLastModified() * 1000, path, reader
                                                        .getLinkInformation(path)
                                                        .tryGetSymbolicLinkTarget()));
                    } else if (info.isDirectory())
                    {
                        entries.add(String.format(
                                "%o\t%d/%d\t       DIR\t%4$tY-%4$tm-%4$td %4$tH:%4$tM:%4$tS\t%5$s",
                                info.getPermissions(), info.getUid(), info.getGid(), info
                                        .getLastModified() * 1000, path));
                    } else
                    {
                        entries.add(String.format(
                                "%o\t%d/%d\t%10d\t%5$tY-%5$tm-%5$td %5$tH:%5$tM:%5$tS\t%6$s", info
                                        .getPermissions(), info.getUid(), info.getGid(), info
                                        .getSize(), info.getLastModified() * 1000, path));
                    }
                } else
                {
                    entries.add(path);
                }
                if (recursive && info.isDirectory())
                {
                    addEntries(reader, entries, path, recursive, verbose, continueOnError);
                }
            }
        } else
        {
            try
            {
                for (HDF5LinkInformation linkInfo : reader.getGroupMemberInformation(dir, false))
                {
                    if (recursive && linkInfo.isGroup())
                    {
                        addEntries(reader, entries, linkInfo.getPath(), recursive, verbose,
                                continueOnError);
                    } else
                    {
                        if (verbose && linkInfo.isDataSet())
                        {
                            entries.add(reader.getDataSetInformation(linkInfo.getPath()).getSize()
                                    + "\t" + linkInfo.getPath());
                        } else
                        {
                            entries.add(linkInfo.getPath());
                        }
                    }
                }
            } catch (HDF5Exception ex)
            {
                HDF5ArchiveTools.dealWithError(new ListArchiveException(dir, ex), continueOnError);
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
