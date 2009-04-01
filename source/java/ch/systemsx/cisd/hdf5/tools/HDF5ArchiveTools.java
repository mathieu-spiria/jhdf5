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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;

import ch.rinn.restrictions.Private;
import ch.systemsx.cisd.base.exceptions.IOExceptionUnchecked;
import ch.systemsx.cisd.base.unix.Unix;
import ch.systemsx.cisd.base.unix.Unix.Group;
import ch.systemsx.cisd.base.unix.Unix.Password;
import ch.systemsx.cisd.hdf5.HDF5GenericCompression;
import ch.systemsx.cisd.hdf5.HDF5OpaqueType;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;

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
    public static void archive(IHDF5Writer writer, ArchivingStrategy strategy, File root,
            File path, boolean continueOnError, boolean verbose) throws IOExceptionUnchecked
    {
        final boolean ok;
        int crc32 = 0;
        if (path.isDirectory())
        {
            ok = archiveDirectory(writer, strategy, root, path, continueOnError, verbose);
        } else if (path.isFile())
        {
            final Link pseudoLinkForChecksum = new Link();
            ok =
                    archiveFile(writer, strategy, root, path, pseudoLinkForChecksum,
                            continueOnError, verbose);
            crc32 = pseudoLinkForChecksum.getCrc32();
        } else
        {
            ok = false;
            dealWithError(new ArchivingException(path, new IOException(
                    "Path corresponds to neither a file nor a directory.")), continueOnError);
        }
        if (ok)
        {
            updateIndicesOnThePath(writer, strategy, root, path, crc32, continueOnError);
        }
    }

    private static void updateIndicesOnThePath(IHDF5Writer writer, ArchivingStrategy strategy,
            File root, File path, int crc32, boolean continueOnError)
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
                    new DirectoryIndex(writer, hdf5GroupPath, continueOnError, false);
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

    private static boolean archiveDirectory(IHDF5Writer writer, ArchivingStrategy strategy,
            File root, File dir, boolean continueOnError, boolean verbose)
            throws ArchivingException
    {
        final File[] fileEntries = dir.listFiles();
        if (fileEntries == null)
        {
            dealWithError(new ArchivingException(dir, new IOException("Cannot read directory")),
                    continueOnError);
            return false;
        }
        final String hdf5GroupPath = getRelativePath(root, dir);
        if (writer.getFileFormat() != FileFormat.STRICTLY_1_8
                && fileEntries.length > MIN_GROUP_MEMBER_COUNT_TO_COMPUTE_SIZEHINT
                && "/.".equals(hdf5GroupPath) == false)
        {
            try
            {
                // Compute size hint and pre-create group in order to improve performance.
                int totalLength = computeSizeHint(fileEntries);
                writer.createGroup(hdf5GroupPath, totalLength * SIZEHINT_FACTOR);
            } catch (HDF5Exception ex)
            {
                dealWithError(new ArchivingException(hdf5GroupPath, ex), continueOnError);
            }
        }
        final List<Link> linkEntries =
                DirectoryIndex.convertFilesToLinks(fileEntries, strategy
                        .doStoreOwnerAndPermissions(), continueOnError);

        writeToConsole(hdf5GroupPath, verbose);
        final Iterator<Link> linkIt = linkEntries.iterator();
        for (int i = 0; i < fileEntries.length; ++i)
        {
            final File file = fileEntries[i];
            final Link link = linkIt.next();
            final String absoluteEntry = file.getAbsolutePath();
            if (link.isDirectory())
            {
                if (strategy.doExclude(absoluteEntry, true))
                {
                    linkIt.remove();
                    continue;
                }
                final boolean ok =
                        archiveDirectory(writer, strategy, root, file, continueOnError, verbose);
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
                if (link.isSymLink())
                {
                    final String linkTargetOrNull = Link.tryReadLinkTarget(file);
                    if (linkTargetOrNull == null)
                    {
                        dealWithError(new ArchivingException(file, new IOException(
                                "Cannot read link target of symbolic link.")), continueOnError);
                    }
                    try
                    {
                        writer.createSoftLink(linkTargetOrNull, hdf5GroupPath + "/"
                                + link.getLinkName());
                    } catch (HDF5Exception ex)
                    {
                        linkIt.remove();
                        dealWithError(new ArchivingException(hdf5GroupPath + "/"
                                + link.getLinkName(), ex), continueOnError);
                    }
                } else if (link.isRegularFile())
                {
                    final boolean ok =
                            archiveFile(writer, strategy, root, file, link, continueOnError,
                                    verbose);
                    if (ok == false)
                    {
                        linkIt.remove();
                    }
                } else
                {
                    dealWithError(new ArchivingException(file, new IOException(
                            "Path corresponds to neither a file nor a directory.")),
                            continueOnError);
                }
            }
        }

        final DirectoryIndex index =
                new DirectoryIndex(writer, hdf5GroupPath, continueOnError, verbose);
        index.addToIndex(linkEntries);
        index.writeIndexToArchive();
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

    private static void writeToConsole(String hdf5ObjectPath, boolean verbose)
    {
        if (verbose)
        {
            System.out.println(hdf5ObjectPath);
        }
    }

    private static void writeToConsole(String hdf5ObjectPath, boolean checksumOK, int crc32,
            boolean verbose)
    {
        if (verbose)
        {
            if (checksumOK)
            {
                System.out.println(hdf5ObjectPath + "\t" + hashToString(crc32) + "\tOK");
            } else
            {
                System.out.println(hdf5ObjectPath + "\t" + hashToString(crc32) + "\tFAILED");
            }
        }
    }

    private static boolean archiveFile(IHDF5Writer writer, ArchivingStrategy strategy, File root,
            File file, Link link, boolean continueOnError, boolean verbose)
            throws ArchivingException
    {
        boolean ok = true;
        final String hdf5ObjectPath = getRelativePath(root, file);
        final HDF5GenericCompression compression = strategy.doCompress(hdf5ObjectPath);
        try
        {
            final long size = file.length();
            final int crc32 = copyToHDF5(file, writer, hdf5ObjectPath, size, compression);
            link.setCrc32(crc32);
            writeToConsole(hdf5ObjectPath, verbose);
        } catch (IOException ex)
        {
            ok = false;
            dealWithError(new ArchivingException(file, ex), continueOnError);
        } catch (HDF5Exception ex)
        {
            ok = false;
            dealWithError(new ArchivingException(hdf5ObjectPath, ex), continueOnError);
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

    /**
     * Extracts the <var>path</var> from the HDF5 archive and stores it relative to <var>root</var>.
     */
    public static void extract(IHDF5Reader reader, ArchivingStrategy strategy, File root,
            String path, boolean continueOnError, boolean verbose) throws UnarchivingException
    {
        final String unixPath = FilenameUtils.separatorsToUnix(path);
        if (reader.exists(unixPath) == false)
        {
            throw new UnarchivingException(unixPath, "Object does not exist in archive.");
        }
        final Link linkOrNull = tryGetLink(reader, unixPath, continueOnError);
        if (reader.isGroup(unixPath))
        {
            extractDirectory(reader, strategy, new GroupCache(), root, unixPath, linkOrNull,
                    continueOnError, verbose);
        } else
        {
            extractFile(reader, strategy, new GroupCache(), root, unixPath, linkOrNull,
                    continueOnError, verbose);
        }
    }

    /**
     * Returns the {@link Link} for <var>path</var> if that is stored in the directory index and
     * <code>null</code> otherwise.
     * <p>
     * <em>Note that a return value of <code>null</code> does not necessarily mean that <var>path</var>
     * is not in the archive!<em>
     */
    private static Link tryGetLink(IHDF5Reader reader, String path, boolean continueOnError)
    {
        final DirectoryIndex index =
                new DirectoryIndex(reader, FilenameUtils.separatorsToUnix(FilenameUtils
                        .getFullPathNoEndSeparator(path)), continueOnError, false);
        return index.tryGetLink(FilenameUtils.getName(path));
    }

    /**
     * Deletes the <var>path</var> from the HDF5 archive.
     */
    @SuppressWarnings("null")
    public static void delete(IHDF5Writer writer, List<String> paths, boolean continueOnError,
            boolean verbose) throws UnarchivingException
    {
        DirectoryIndex indexOrNull = null;
        String lastGroupOrNull = null;
        for (String path : paths)
        {
            String normalizedPath = path;
            if (normalizedPath.endsWith("/"))
            {
                normalizedPath = normalizedPath.substring(0, path.length() - 1);
            }
            int groupDelimIndex = normalizedPath.lastIndexOf('/');
            final String group =
                    (groupDelimIndex < 2) ? "/" : normalizedPath.substring(0, groupDelimIndex);
            if (group.equals(lastGroupOrNull) == false)
            {
                if (indexOrNull != null)
                {
                    indexOrNull.writeIndexToArchive();
                }
                indexOrNull = new DirectoryIndex(writer, group, continueOnError, false);
            }
            try
            {
                writer.delete(normalizedPath);
                final String name = normalizedPath.substring(groupDelimIndex + 1);
                indexOrNull.remove(name);
                writeToConsole(normalizedPath, verbose);
            } catch (HDF5Exception ex)
            {
                HDF5ArchiveTools.dealWithError(new DeleteFromArchiveException(path, ex),
                        continueOnError);
            }
        }
        if (indexOrNull != null)
        {
            indexOrNull.writeIndexToArchive();
        }
    }

    private static void extractDirectory(IHDF5Reader reader, ArchivingStrategy strategy,
            GroupCache groupCache, File root, String groupPath, Link dirLinkOrNull,
            boolean continueOnError, boolean verbose) throws UnarchivingException
    {
        String objectPathOrNull = null;
        try
        {
            final File groupFile = new File(root, groupPath);
            groupFile.mkdir();
            for (Link link : new DirectoryIndex(reader, groupPath, continueOnError, false))
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
                    writeToConsole(objectPathOrNull, verbose);
                    extractDirectory(reader, strategy, groupCache, root, objectPathOrNull, link,
                            continueOnError, verbose);
                } else if (link.isRegularFile() || link.isSymLink())
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
            }
            restoreAttributes(groupFile, dirLinkOrNull, groupCache);
        } catch (HDF5Exception ex)
        {
            dealWithError(new UnarchivingException(objectPathOrNull == null ? groupPath
                    : objectPathOrNull, ex), continueOnError);
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

    private static void extractFile(IHDF5Reader reader, ArchivingStrategy strategy,
            GroupCache groupCache, File root, String hdf5ObjectPath, Link linkOrNull,
            boolean continueOnError, boolean verbose) throws UnarchivingException
    {
        if (strategy.doExclude(hdf5ObjectPath, false))
        {
            return;
        }
        final File file = new File(root, hdf5ObjectPath);
        file.getParentFile().mkdirs();
        if (linkOrNull != null && linkOrNull.isSymLink())
        {
            if (Unix.isOperational())
            {
                try
                {
                    String linkTargetOrNull = linkOrNull.tryGetLinkTarget();
                    if (linkTargetOrNull == null)
                    {
                        linkTargetOrNull =
                                reader.getLinkInformation(hdf5ObjectPath)
                                        .tryGetSymbolicLinkTarget();
                    }
                    if (linkTargetOrNull == null)
                    {
                        dealWithError(new UnarchivingException(hdf5ObjectPath,
                                "Cannot extract symlink as no link target stored."),
                                continueOnError);
                    } else
                    {
                        Unix.createSymbolicLink(linkTargetOrNull, file.getAbsolutePath());
                        writeToConsole(hdf5ObjectPath, verbose);
                    }
                } catch (IOExceptionUnchecked ex)
                {
                    dealWithError(new UnarchivingException(file, ex), continueOnError);
                } catch (HDF5Exception ex)
                {
                    dealWithError(new UnarchivingException(hdf5ObjectPath, ex), continueOnError);
                }
                return;
            } else
            {
                System.err.println("Warning: extracting symlink as regular file as Unix calls are "
                        + "not available on this system.");
            }
        }
        final int storedCrc32;
        if (linkOrNull != null)
        {
            storedCrc32 = linkOrNull.getCrc32();
        } else
        {
            storedCrc32 = 0;
        }
        try
        {
            final long size = reader.getDataSetInformation(hdf5ObjectPath).getSize();
            final int crc32 = copyFromHDF5(reader, hdf5ObjectPath, size, file);
            restoreAttributes(file, linkOrNull, groupCache);
            final boolean checksumOK = (crc32 == storedCrc32);
            writeToConsole(hdf5ObjectPath, checksumOK, crc32, verbose);
            if (checksumOK == false)
            {
                dealWithError(new UnarchivingException(hdf5ObjectPath,
                        "CRC checksum mismatch. Expected: " + hashToString(storedCrc32)
                                + ", found: " + hashToString(crc32)), continueOnError);
            }
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

    @Private
    static String describeLink(String path, Link link, IdCache idCache, boolean verbose,
            boolean numeric)
    {
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
                    return String
                            .format(
                                    "%s\t%s\t%s\t%10d\t%5$tY-%5$tm-%5$td %5$tH:%5$tM:%5$tS\t%6$s%7$s\t%8$s",
                                    getPermissions(link, numeric), idCache.getUser(link, numeric),
                                    idCache.getGroup(link, numeric), link.getSize(), link
                                            .getLastModified()
                                            * MILLIS_PER_SECOND, path, link.isRegularFile() ? ""
                                            : "\t*", hashToString(link.getCrc32()));
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
     * One entry of the listing. Contains a list for output and
     */
    static class ListEntry
    {
        final String outputLine;

        final int crc32Expected;

        final int crc32Found;

        ListEntry(String outputLine, int crc32Expected, int crc32Found)
        {
            this.outputLine = outputLine;
            this.crc32Expected = crc32Expected;
            this.crc32Found = crc32Found;
        }
    }

    /**
     * Returns a listing of entries in <var>dir</var> in the archive provided by <var>reader</var>.
     */
    public static List<ListEntry> list(IHDF5Reader reader, String dir, boolean recursive,
            boolean verbose, boolean numeric, boolean testAgainstChecksum, boolean continueOnError)
    {
        final List<ListEntry> result = new LinkedList<ListEntry>();
        addEntries(reader, result, FilenameUtils.separatorsToUnix(dir), new IdCache(), recursive,
                verbose, numeric, testAgainstChecksum, continueOnError);
        return result;
    }

    /**
     * Adds the entries of <var>dir</var> to <var>entries</var> recursively.
     */
    private static void addEntries(IHDF5Reader reader, List<ListEntry> entries, String dir,
            IdCache idCache, boolean recursive, boolean verbose, boolean numeric,
            boolean testAgainstChecksum, boolean continueOnError)
    {
        if (reader.exists(dir) == false)
        {
            HDF5ArchiveTools.dealWithError(new ListArchiveException(dir, new HDF5JavaException(
                    "Directory not found in archive.")), continueOnError);
            return;
        }
        final String dirPrefix = dir.endsWith("/") ? dir : (dir + "/");
        for (Link link : new DirectoryIndex(reader, dir, continueOnError, verbose))
        {
            final String path = dirPrefix + link.getLinkName();
            final int crc32 =
                    (testAgainstChecksum && link.isRegularFile()) ? calcCRC32(reader, path, link
                            .getSize()) : 0;
            entries.add(new ListEntry(describeLink(path, link, idCache, verbose, numeric), link
                    .getCrc32(), crc32));
            if (recursive && link.isDirectory() && "/.".equals(path) == false)
            {
                addEntries(reader, entries, path, idCache, recursive, verbose, numeric,
                        testAgainstChecksum, continueOnError);
            }
        }
    }

    private static int calcCRC32(IHDF5Reader reader, String objectPath, long size)
    {
        final CRC32 crc32Digest = new CRC32();
        final long blockCount =
                (size / FILE_SIZE_THRESHOLD) + (size % FILE_SIZE_THRESHOLD != 0 ? 1 : 0);
        for (int i = 0; i < blockCount; ++i)
        {
            final byte[] buffer = reader.readAsByteArrayBlock(objectPath, FILE_SIZE_THRESHOLD, i);
            crc32Digest.update(buffer);
        }
        return (int) crc32Digest.getValue();
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

    private static int copyToHDF5(File source, final IHDF5Writer writer, final String objectPath,
            final long size, final HDF5GenericCompression compression) throws IOException
    {
        final InputStream input = FileUtils.openInputStream(source);
        final byte[] buffer = new byte[FILE_SIZE_THRESHOLD];
        final int blockSize = (int) Math.min(size, FILE_SIZE_THRESHOLD);
        final HDF5OpaqueType type;
        if (writer.exists(objectPath))
        {
            type = writer.tryGetOpaqueType(objectPath);
            if (type == null || OPAQUE_TAG_FILE.equals(type.getTag()) == false)
            {
                throw new HDF5JavaException("Object " + objectPath + " is not an opaque type '"
                        + OPAQUE_TAG_FILE + "'");
            }
        } else
        {
            type =
                    writer.createOpaqueByteArray(objectPath, OPAQUE_TAG_FILE, size, blockSize,
                            compression);

        }
        final CRC32 crc32 = new CRC32();
        try
        {
            long count = 0;
            int n = 0;
            while (-1 != (n = input.read(buffer)))
            {
                writer.writeOpaqueByteArrayBlockWithOffset(objectPath, type, buffer, n, count);
                count += n;
                crc32.update(buffer, 0, n);
            }
        } finally
        {
            IOUtils.closeQuietly(input);
        }
        return (int) crc32.getValue();
    }

    private static int copyFromHDF5(IHDF5Reader reader, final String objectPath, final long size,
            File destination) throws IOException
    {
        final OutputStream output = FileUtils.openOutputStream(destination);
        final CRC32 crc32 = new CRC32();
        try
        {
            final long blockCount =
                    (size / FILE_SIZE_THRESHOLD) + (size % FILE_SIZE_THRESHOLD != 0 ? 1 : 0);
            for (int i = 0; i < blockCount; ++i)
            {
                final byte[] buffer =
                        reader.readAsByteArrayBlock(objectPath, FILE_SIZE_THRESHOLD, i);
                output.write(buffer);
                crc32.update(buffer);
            }
            output.close(); // Make sure we don't silence exceptions on closing.
        } finally
        {
            IOUtils.closeQuietly(output);
        }
        return (int) crc32.getValue();
    }

    private static final char[] HEX_CHARACTERS =
        { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', };

    static String hashToString(final int checksum)
    {
        if (checksum == 0)
        {
            return "-";
        }
        final char buf[] = new char[8];
        int w = checksum;
        for (int i = 0, x = 7; i < 4; i++)
        {
            buf[x--] = HEX_CHARACTERS[w & 0xf];
            buf[x--] = HEX_CHARACTERS[(w >>> 4) & 0xf];
            w >>= 8;
        }
        return new String(buf);
    }

}
