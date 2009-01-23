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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;

import ch.rinn.restrictions.Private;
import ch.systemsx.cisd.common.exceptions.WrappedIOException;
import ch.systemsx.cisd.common.os.FileLinkType;
import ch.systemsx.cisd.common.os.Unix;
import ch.systemsx.cisd.common.os.Unix.Group;
import ch.systemsx.cisd.common.os.Unix.Password;
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
    private static final long MILLIS_PER_SECOND = 1000L;

    private static final int ROOT_UID = 0;

    private static final String OPAQUE_TAG_FILE = "FILE";

    private static final String OPAQUE_TAG_INDEX = "INDEX";

    private static final int SIZEHINT_FACTOR = 5;

    private static final int MIN_GROUP_MEMBER_COUNT_TO_COMPUTE_SIZEHINT = 100;

    private final static int MB = 1024 * 1024;

    /**
     * Threshold for switching to block-wise I/O.
     */
    private final static int FILE_SIZE_THRESHOLD = 10 * MB;

    /**
     * Cache for group affiliations of the current user.
     */
    private static class GroupCache
    {
        private final Password userOrNull;

        /** Gid -> Is user member? */
        private final Map<Integer, Boolean> gidMap = new HashMap<Integer, Boolean>();

        GroupCache()
        {
            this.userOrNull = Unix.isOperational() ? Unix.tryGetUserByUid(Unix.getUid()) : null;
        }

        boolean isUserInGroup(int gid)
        {
            if (userOrNull == null)
            {
                return false;
            }
            final Boolean cached = gidMap.get(gid);
            if (cached != null)
            {
                return cached;
            }
            final Group groupOrNull = Unix.tryGetGroupByGid(gid);
            if (groupOrNull != null)
            {
                final int idx =
                        ArrayUtils.indexOf(groupOrNull.getGroupMembers(), userOrNull.getUserName());
                final Boolean found =
                        idx != ArrayUtils.INDEX_NOT_FOUND ? Boolean.TRUE : Boolean.FALSE;
                gidMap.put(gid, found);
                return found;
            } else
            {
                gidMap.put(gid, Boolean.FALSE);
                return false;
            }
        }
    }

    /**
     * Cache for ID -> Name mapping.
     * 
     * @author Bernd Rinn
     */
    @Private
    static class IdCache
    {
        /** Gid -> Group Name */
        private final Map<Integer, String> gidMap = new HashMap<Integer, String>();

        /** Uid -> User Name */
        private final Map<Integer, String> uidMap = new HashMap<Integer, String>();

        /**
         * Returns the name for the given <var>uid</var>.
         */
        String getUser(Link link, boolean numeric)
        {
            final int uid = link.getUid();
            String userNameOrNull = uidMap.get(uidMap);
            if (userNameOrNull == null)
            {
                userNameOrNull =
                        (numeric == false && Unix.isOperational()) ? Unix.tryGetUserNameForUid(uid)
                                : null;
                if (userNameOrNull == null)
                {
                    userNameOrNull = Integer.toString(uid);
                }
                uidMap.put(uid, userNameOrNull);
            }
            return userNameOrNull;
        }

        /**
         * Returns the name for the given <var>gid</var>.
         */
        String getGroup(Link link, boolean numeric)
        {
            final int gid = link.getGid();
            String groupNameOrNull = gidMap.get(uidMap);
            if (groupNameOrNull == null)
            {
                groupNameOrNull =
                        (numeric == false && Unix.isOperational()) ? Unix
                                .tryGetGroupNameForGid(gid) : null;
                if (groupNameOrNull == null)
                {
                    groupNameOrNull = Integer.toString(gid);
                }
                gidMap.put(gid, groupNameOrNull);
            }
            return groupNameOrNull;
        }
    }

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

        writeToConsole(hdf5GroupPath, verbose);
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

    private static LinkInfo[] tryReadIndex(final HDF5Reader reader, final String hdf5GroupPath)
    {
        final String indexDataSetName = getIndexDataSetName(hdf5GroupPath);
        if (reader.exists(indexDataSetName))
        {
            return LinkInfo.fromStorageForm(reader.readAsByteArray(indexDataSetName));
        } else
        {
            return null;
        }
    }

    private static Map<String, LinkInfo> tryGetIndexMap(final HDF5Reader reader,
            final String hdf5GroupPath)
    {
        LinkInfo[] infos = tryReadIndex(reader, hdf5GroupPath);
        if (infos == null)
        {
            if (reader.isGroup(hdf5GroupPath))
            {
                final List<HDF5LinkInformation> hdf5LinkInfos =
                        reader.getGroupMemberInformation(hdf5GroupPath, false);
                infos = new LinkInfo[hdf5LinkInfos.size()];
                for (int i = 0; i < hdf5LinkInfos.size(); ++i)
                {
                    final HDF5LinkInformation linfo = hdf5LinkInfos.get(i);
                    final long size = reader.getDataSetInformation(linfo.getPath()).getSize();
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

    static FileLinkType translateType(final HDF5ObjectType hdf5Type)
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

    static void writeToConsole(String hdf5ObjectPath, boolean verbose)
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
            writeToConsole(hdf5ObjectPath, verbose);
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
        final Link linkOrNull = getLinkForPath(reader, path);
        if (reader.isGroup(path))
        {
            extractDirectory(reader, strategy, new GroupCache(), root, path, linkOrNull,
                    continueOnError, verbose);
        } else
        {
            extractFile(reader, strategy, new GroupCache(), root, path, linkOrNull,
                    continueOnError, verbose);
        }
    }

    private static Link getLinkForPath(HDF5Reader reader, String path)
    {
        final Map<String, LinkInfo> indexMapOrNull =
                tryGetIndexMap(reader, FilenameUtils.getFullPathNoEndSeparator(path));
        final LinkInfo linkInfoOrNull =
                (indexMapOrNull != null) ? indexMapOrNull.get(FilenameUtils.getName(path)) : null;
        return (linkInfoOrNull != null) ? new Link(linkInfoOrNull, null) : null;
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
                writeToConsole(path, verbose);
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

    private static void extractDirectory(HDF5Reader reader, ArchivingStrategy strategy,
            GroupCache groupCache, File root, String groupPath, Link dirLinkOrNull,
            boolean continueOnError, boolean verbose) throws UnarchivingException
    {
        String objectPathOrNull = null;
        try
        {
            final File groupFile = new File(root, groupPath);
            groupFile.mkdir();
            for (Link link : getLinks(reader, groupPath, false, continueOnError))
            {
                objectPathOrNull = FilenameUtils.concat(groupPath, link.getLinkName());
                if (link.isDirectory())
                {
                    if (strategy.doExclude(objectPathOrNull, true))
                    {
                        continue;
                    }
                    writeToConsole(objectPathOrNull, verbose);
                    extractDirectory(reader, strategy, groupCache, root, objectPathOrNull, link,
                            continueOnError, verbose);
                } else if (link.isRegularFile())
                {
                    extractFile(reader, strategy, groupCache, root, objectPathOrNull, link,
                            continueOnError, verbose);
                } else
                {
                    dealWithError(new UnarchivingException(objectPathOrNull,
                            "Unexpected object type: "
                                    + tryGetObjectTypeDescriptionForErrorMessage(reader,
                                            objectPathOrNull) + "."), continueOnError);
                }
                restoreAttributes(groupFile, dirLinkOrNull, groupCache);
            }
        } catch (HDF5Exception ex)
        {
            dealWithError(new UnarchivingException(objectPathOrNull == null ? groupPath
                    : objectPathOrNull, ex), continueOnError);
        }
    }

    private static String tryGetObjectTypeDescriptionForErrorMessage(HDF5Reader reader,
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

    private static void extractFile(HDF5Reader reader, ArchivingStrategy strategy,
            GroupCache groupCache, File root, String hdf5ObjectPath, Link linkOrNull,
            boolean continueOnError, boolean verbose) throws UnarchivingException
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
            restoreAttributes(file, linkOrNull, groupCache);
            writeToConsole(hdf5ObjectPath, verbose);
        } catch (IOException ex)
        {
            dealWithError(new UnarchivingException(file, ex), continueOnError);
        } catch (HDF5Exception ex)
        {
            dealWithError(new UnarchivingException(hdf5ObjectPath, ex), continueOnError);
        }
    }

    private static void restoreAttributes(File file, Link linkInfoOrNull, GroupCache groupCache)
    {
        assert file != null;

        if (linkInfoOrNull != null)
        {
            if (linkInfoOrNull.hasLastModified())
            {
                file.setLastModified(linkInfoOrNull.getLastModified() * MILLIS_PER_SECOND);
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

    private static List<Link> getLinks(HDF5Reader reader, String dir, boolean verbose,
            boolean continueOnError)
    {
        final String indexDataSetName = getIndexDataSetName(dir);
        final List<Link> result = new LinkedList<Link>();
        final String dirPrefix = dir.endsWith("/") ? dir : (dir + "/");
        if (reader.exists(indexDataSetName))
        {
            final LinkInfo[] infos =
                    LinkInfo.fromStorageForm(reader.readAsByteArray(indexDataSetName));
            for (LinkInfo info : infos)
            {
                try
                {
                    final String path = dirPrefix + info.getLinkName();
                    final String linkTargetOrNull =
                            (verbose && info.isSymLink()) ? reader.getLinkInformation(path)
                                    .tryGetSymbolicLinkTarget() : null;
                    result.add(new Link(info, linkTargetOrNull));
                } catch (HDF5Exception ex)
                {
                    HDF5ArchiveTools.dealWithError(new ListArchiveException(dir, ex),
                            continueOnError);
                }
            }
        } else
        {
            for (HDF5LinkInformation info : reader.getGroupMemberInformation(dir, verbose))
            {
                try
                {
                    result.add(new Link(info, verbose ? reader
                            .getDataSetInformation(info.getPath()).getSize() : LinkInfo.UNKNOWN,
                            LinkInfo.UNKNOWN));
                } catch (HDF5Exception ex)
                {
                    HDF5ArchiveTools.dealWithError(new ListArchiveException(dir, ex),
                            continueOnError);
                }
            }
        }
        return result;
    }

    @Private
    static String describeLink(String dir, Link link, IdCache idCache, boolean verbose,
            boolean numeric)
    {
        final String path = dir + link.getLinkName();
        if (verbose == false)
        {
            return path;
        }
        switch (link.getCompleteness())
        {
            case BASE:
                if (link.isSymLink())
                {
                    return String.format("          \t%s -> %s", path, link.tryGetLinkTarget());
                } else if (link.isDirectory())
                {
                    return String.format("       DIR\t%s", path);
                } else
                {
                    return String.format("%10d\t%s%s", link.getSize(), path,
                            link.isRegularFile() ? "" : "\t*");
                }
            case LAST_MODIFIED:
                if (link.isSymLink())
                {
                    return String.format(
                            "          \t%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS\t%2$s -> %3$s", link
                                    .getLastModified()
                                    * MILLIS_PER_SECOND, path, link.tryGetLinkTarget());
                } else if (link.isDirectory())
                {
                    return String.format("       DIR\t%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS\t%2$s",
                            link.getLastModified() * MILLIS_PER_SECOND, path);
                } else
                {
                    return String.format("%10d\t%2$tY-%2$tm-%2$td %2$tH:%2$tM:%2$tS\t%3$s%4$s",
                            link.getSize(), link.getLastModified() * MILLIS_PER_SECOND, path, link
                                    .isRegularFile() ? "" : "\t*");
                }
            case FULL:
                if (link.isSymLink())
                {
                    return String
                            .format(
                                    "%s\t%s\t%s\t          \t%4$tY-%4$tm-%4$td %4$tH:%4$tM:%4$tS\t%5$s -> %6$s",
                                    getPermissions(link, numeric), idCache.getUser(link, numeric),
                                    idCache.getGroup(link, numeric), link.getLastModified()
                                            * MILLIS_PER_SECOND, path, link.tryGetLinkTarget());
                } else if (link.isDirectory())
                {
                    return String.format(
                            "%s\t%s\t%s\t       DIR\t%4$tY-%4$tm-%4$td %4$tH:%4$tM:%4$tS\t%5$s",
                            getPermissions(link, numeric), idCache.getUser(link, numeric), idCache
                                    .getGroup(link, numeric), link.getLastModified()
                                    * MILLIS_PER_SECOND, path);
                } else
                {
                    return String.format(
                            "%s\t%s\t%s\t%10d\t%5$tY-%5$tm-%5$td %5$tH:%5$tM:%5$tS\t%6$s%7$s",
                            getPermissions(link, numeric), idCache.getUser(link, numeric), idCache
                                    .getGroup(link, numeric), link.getSize(), link
                                    .getLastModified()
                                    * MILLIS_PER_SECOND, path, link.isRegularFile() ? "" : "\t*");
                }
            default:
                throw new Error("Unknown level of link information completeness: "
                        + link.getCompleteness());
        }
    }

    @Private
    static String getPermissions(Link link, boolean numeric)
    {
        if (numeric)
        {
            return Integer.toString(link.getPermissions(), 8);
        } else
        {
            final short perms = link.getPermissions();
            final StringBuilder b = new StringBuilder();
            b.append(link.isDirectory() ? 'd' : '-');
            b.append((perms & Unix.S_IRUSR) != 0 ? 'r' : '-');
            b.append((perms & Unix.S_IWUSR) != 0 ? 'w' : '-');
            b.append((perms & Unix.S_IXUSR) != 0 ? ((perms & Unix.S_ISUID) != 0 ? 's' : 'x')
                    : ((perms & Unix.S_ISUID) != 0 ? 'S' : '-'));
            b.append((perms & Unix.S_IRGRP) != 0 ? 'r' : '-');
            b.append((perms & Unix.S_IWGRP) != 0 ? 'w' : '-');
            b.append((perms & Unix.S_IXGRP) != 0 ? ((perms & Unix.S_ISGID) != 0 ? 's' : 'x')
                    : ((perms & Unix.S_ISGID) != 0 ? 'S' : '-'));
            b.append((perms & Unix.S_IROTH) != 0 ? 'r' : '-');
            b.append((perms & Unix.S_IWOTH) != 0 ? 'w' : '-');
            b.append((perms & Unix.S_IXOTH) != 0 ? ((perms & Unix.S_ISVTX) != 0 ? 't' : 'x')
                    : ((perms & Unix.S_ISVTX) != 0 ? 'T' : '-'));
            return b.toString();
        }
    }

    /**
     * Returns a listing of entries in <var>dir</var> in the archive provided by <var>reader</var>.
     */
    public static List<String> list(HDF5Reader reader, String dir, boolean recursive,
            boolean verbose, boolean numeric, boolean continueOnError)
    {
        final List<String> result = new LinkedList<String>();
        addEntries(reader, result, dir, new IdCache(), recursive, verbose, numeric, continueOnError);
        return result;
    }

    /**
     * Adds the entries of <var>dir</var> to <var>entries</var> recursively.
     */
    private static void addEntries(HDF5Reader reader, List<String> entries, String dir,
            IdCache idCache, boolean recursive, boolean verbose, boolean numeric,
            boolean continueOnError)
    {
        if (reader.exists(dir) == false)
        {
            HDF5ArchiveTools.dealWithError(new ListArchiveException(dir, new HDF5JavaException(
                    "Directory not found in archive.")), continueOnError);
            return;
        }
        final String dirPrefix = dir.endsWith("/") ? dir : (dir + "/");
        for (Link link : getLinks(reader, dir, verbose, continueOnError))
        {
            entries.add(describeLink(dirPrefix, link, idCache, verbose, numeric));
            if (recursive && link.isDirectory())
            {
                addEntries(reader, entries, dirPrefix + link.getLinkName(), idCache, recursive,
                        verbose, numeric, continueOnError);
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
