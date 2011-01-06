/*
 * Copyright 2008 ETH Zuerich, CISD
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * Copyright 2008 ETH Zuerich, CISD
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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import ch.systemsx.cisd.base.unix.Unix.Stat;
import ch.systemsx.cisd.base.utilities.OSUtilities;
import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
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
            return getUser(link.getUid(), numeric);
        }

        /**
         * Returns the name for the given <var>uid</var>.
         */
        String getUser(Stat link, boolean numeric)
        {
            return getUser(link.getUid(), numeric);
        }

        private String getUser(int uid, boolean numeric)
        {
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
            return getGroup(link.getGid(), numeric);
        }

        /**
         * Returns the name for the given <var>gid</var>.
         */
        String getGroup(Stat link, boolean numeric)
        {
            return getGroup(link.getGid(), numeric);
        }

        /**
         * Returns the name for the given <var>gid</var>.
         */
        private String getGroup(int gid, boolean numeric)
        {
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
            File path, boolean continueOnError, boolean verbose, byte[] buffer)
            throws IOExceptionUnchecked
    {
        final boolean ok;
        int crc32 = 0;
        if (path.isDirectory())
        {
            ok = archiveDirectory(writer, strategy, root, path, continueOnError, verbose, buffer);
        } else if (path.isFile())
        {
            final Link pseudoLinkForChecksum = new Link();
            ok =
                    archiveFile(writer, strategy, root, path, pseudoLinkForChecksum,
                            continueOnError, verbose, buffer);
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
            File root, File dir, boolean continueOnError, boolean verbose, byte[] buffer)
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
                DirectoryIndex.convertFilesToLinks(fileEntries,
                        strategy.doStoreOwnerAndPermissions(), continueOnError);

        writeToConsole(hdf5GroupPath, verbose);
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
                final boolean ok =
                        archiveDirectory(writer, strategy, root, file, continueOnError, verbose,
                                buffer);
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
                    final String linkTargetOrNull = Link.tryReadLinkTarget(file);
                    if (linkTargetOrNull == null)
                    {
                        dealWithError(new ArchivingException(file, new IOException(
                                "Cannot read link target of symbolic link.")), continueOnError);
                    }
                    try
                    {
                        writer.createSoftLink(linkTargetOrNull,
                                hdf5GroupPath + "/" + linkOrNull.getLinkName());
                    } catch (HDF5Exception ex)
                    {
                        linkIt.remove();
                        dealWithError(
                                new ArchivingException(hdf5GroupPath + "/"
                                        + linkOrNull.getLinkName(), ex), continueOnError);
                    }
                } else if (linkOrNull.isRegularFile())
                {
                    final boolean ok =
                            archiveFile(writer, strategy, root, file, linkOrNull, continueOnError,
                                    verbose, buffer);
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
            File file, Link link, boolean continueOnError, boolean verbose, byte[] buffer)
            throws ArchivingException
    {
        boolean ok = true;
        final String hdf5ObjectPath = getRelativePath(root, file);
        final HDF5GenericStorageFeatures compression = strategy.doCompress(hdf5ObjectPath);
        try
        {
            final long size = file.length();
            final int crc32 = copyToHDF5(file, writer, hdf5ObjectPath, size, compression, buffer);
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
            String path, boolean continueOnError, boolean verbose, byte[] buffer)
            throws UnarchivingException
    {
        final String unixPath = FilenameUtils.separatorsToUnix(path);
        if (reader.exists(unixPath, false) == false)
        {
            throw new UnarchivingException(unixPath, "Object does not exist in archive.");
        }
        final Link linkOrNull = tryGetLink(reader, unixPath, continueOnError);
        final boolean isDir =
                (linkOrNull != null && linkOrNull.isDirectory())
                        || ((linkOrNull == null && reader.isGroup(unixPath, false)));
        if (isDir)
        {
            extractDirectory(reader, strategy, new GroupCache(), root, unixPath, linkOrNull,
                    continueOnError, verbose, buffer);
        } else
        {
            extractFile(reader, strategy, new GroupCache(), root, unixPath, linkOrNull,
                    continueOnError, verbose, buffer);
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
            boolean continueOnError, boolean verbose, byte[] buffer) throws UnarchivingException
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
                            continueOnError, verbose, buffer);
                } else if (link.isRegularFile() || link.isSymLink())
                {
                    extractFile(reader, strategy, groupCache, root, objectPathOrNull, link,
                            continueOnError, verbose, buffer);
                } else
                {
                    dealWithError(
                            new UnarchivingException(objectPathOrNull, "Unexpected object type: "
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
            boolean continueOnError, boolean verbose, byte[] buffer) throws UnarchivingException
    {
        if (strategy.doExclude(hdf5ObjectPath, false))
        {
            return;
        }
        final File file = new File(root, hdf5ObjectPath);
        file.getParentFile().mkdirs();
        final boolean isSymLink =
                (linkOrNull != null && linkOrNull.isSymLink())
                        || (linkOrNull == null && reader.isSoftLink(hdf5ObjectPath));
        if (isSymLink)
        {
            if (Unix.isOperational())
            {
                try
                {
                    final String linkTargetOrNull =
                            (linkOrNull != null) ? linkOrNull.tryGetLinkTarget() : reader
                                    .tryGetSymbolicLinkTarget(hdf5ObjectPath);
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
                System.err.println("Warning: extracting symlink as regular file because"
                        + " Unix calls are not available on this system.");
            }
        }
        final int storedCrc32 = (linkOrNull != null) ? linkOrNull.getCrc32() : 0;
        try
        {
            final long size = reader.getDataSetInformation(hdf5ObjectPath).getSize();
            final int crc32 = copyFromHDF5(reader, hdf5ObjectPath, size, file, buffer);
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
                    return String.format("%10d\t%s\t%s%s", link.getSize(),
                            hashToString(link.getCrc32()), path, link.isRegularFile() ? "" : "\t*");
                }
            case LAST_MODIFIED:
                if (link.isSymLink())
                {
                    return String.format(
                            "          \t%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS\t%2$s -> %3$s",
                            link.getLastModified() * MILLIS_PER_SECOND, path,
                            link.tryGetLinkTarget());
                } else if (link.isDirectory())
                {
                    return String.format("       DIR\t%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS\t%2$s",
                            link.getLastModified() * MILLIS_PER_SECOND, path);
                } else
                {
                    return String.format(
                            "%10d\t%2$tY-%2$tm-%2$td %2$tH:%2$tM:%2$tS\t%3$s\t%4$s%5$s",
                            link.getSize(), link.getLastModified() * MILLIS_PER_SECOND,
                            hashToString(link.getCrc32()), path, link.isRegularFile() ? "" : "\t*");
                }
            case FULL:
                if (link.isSymLink())
                {
                    return String
                            .format("%s\t%s\t%s\t          \t%4$tY-%4$tm-%4$td %4$tH:%4$tM:%4$tS\t00000000\t%5$s -> %6$s",
                                    getPermissionString(link, numeric),
                                    idCache.getUser(link, numeric),
                                    idCache.getGroup(link, numeric), link.getLastModified()
                                            * MILLIS_PER_SECOND, path, link.tryGetLinkTarget());
                } else if (link.isDirectory())
                {
                    return String
                            .format("%s\t%s\t%s\t       DIR\t%4$tY-%4$tm-%4$td %4$tH:%4$tM:%4$tS\t        \t%5$s",
                                    getPermissionString(link, numeric),
                                    idCache.getUser(link, numeric),
                                    idCache.getGroup(link, numeric), link.getLastModified()
                                            * MILLIS_PER_SECOND, path);
                } else
                {
                    return String
                            .format("%s\t%s\t%s\t%10d\t%5$tY-%5$tm-%5$td %5$tH:%5$tM:%5$tS\t%6$s\t%7$s%8$s",
                                    getPermissionString(link, numeric),
                                    idCache.getUser(link, numeric),
                                    idCache.getGroup(link, numeric), link.getSize(),
                                    link.getLastModified() * MILLIS_PER_SECOND,
                                    hashToString(link.getCrc32()), path,
                                    (link.isRegularFile() || link.isDirectory()) ? "" : "\t*");
                }
            default:
                throw new Error("Unknown level of link information completeness: "
                        + link.getCompleteness());
        }
    }

    @Private
    static String getPermissionString(Link link, boolean numeric)
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

        final String errorLineOrNull;

        ListEntry(String outputLine, String errorLineOrNull)
        {
            this.outputLine = outputLine;
            this.errorLineOrNull = errorLineOrNull;
        }

        boolean checkOK()
        {
            return errorLineOrNull == null;
        }
    }

    /**
     * An enumeration for the checks to be performed while running
     * {@link HDF5ArchiveTools#list(IHDF5Reader, ListParameters, ListEntryVisitor, boolean, byte[])}
     * .
     */
    public enum Check
    {
        /** Do not perform any check. */
        NO_CHECK,
        /** Check CRC32 checksums against archive content. */
        CHECK_CRC_ARCHIVE,
        /** Verify CRC32 checksums against the file system. */
        VERIFY_CRC_FS,
        /** Verify CRC32 checksums and attributes against the file system. */
        VERIFY_CRC_ATTR_FS;
    }

    public static final Set<Check> VERIFY_FS = EnumSet.of(Check.VERIFY_CRC_FS,
            Check.VERIFY_CRC_ATTR_FS);

    /**
     * A class to hold all parameters for the listing operation.
     */
    public static final class ListParameters
    {
        private String fileOrDirectoryInArchive;

        private String directoryOnFileSystem;

        private ArchivingStrategy strategy;

        private boolean recursive;

        private boolean verbose;

        private boolean numeric;

        private boolean suppressDirectoryEntries;

        private Check check = Check.NO_CHECK;

        public ListParameters fileOrDirectoryInArchive(String newDirectoryInArchive)
        {
            this.fileOrDirectoryInArchive = newDirectoryInArchive;
            return this;
        }

        public ListParameters directoryOnFileSystem(String newDirectoryOnFileSystem)
        {
            this.directoryOnFileSystem = newDirectoryOnFileSystem;
            return this;
        }

        public ListParameters strategy(ArchivingStrategy newStrategy)
        {
            this.strategy = newStrategy;
            return this;
        }

        public ListParameters recursive(boolean newRecursive)
        {
            this.recursive = newRecursive;
            return this;
        }

        public ListParameters suppressDirectoryEntries(boolean newSuppressDirectories)
        {
            this.suppressDirectoryEntries = newSuppressDirectories;
            return this;
        }

        public ListParameters verbose(boolean newVerbose)
        {
            this.verbose = newVerbose;
            return this;
        }

        public ListParameters numeric(boolean newNumeric)
        {
            this.numeric = newNumeric;
            return this;
        }

        public ListParameters check(Check newCheck)
        {
            this.check = newCheck;
            return this;
        }

        public void check()
        {
            if (fileOrDirectoryInArchive == null)
            {
                throw new NullPointerException("fileOrDirectoryInArchive most not be null.");
            }
            fileOrDirectoryInArchive = FilenameUtils.separatorsToUnix(fileOrDirectoryInArchive);
            if (VERIFY_FS.contains(check) && directoryOnFileSystem == null)
            {
                throw new NullPointerException(
                        "fileOrDirectoryOnFileSystem most not be null when verifying.");
            }
            if (check == null)
            {
                throw new NullPointerException("check most not be null.");
            }
        }

        public String getFileOrDirectoryInArchive()
        {
            return fileOrDirectoryInArchive;
        }

        public String getFileOrDirectoryOnFileSystem()
        {
            return directoryOnFileSystem;
        }

        public ArchivingStrategy getStrategy()
        {
            return strategy;
        }

        public boolean isRecursive()
        {
            return recursive;
        }

        public boolean isSuppressDirectoryEntries()
        {
            return suppressDirectoryEntries;
        }

        public boolean isVerbose()
        {
            return verbose;
        }

        public boolean isNumeric()
        {
            return numeric;
        }

        public Check getCheck()
        {
            return check;
        }
    }

    /**
     * An entry to visit {@Link ListEntry}s.
     */
    public interface ListEntryVisitor
    {
        public void visit(ListEntry entry);
    }

    /**
     * Returns a listing of entries in <var>dir</var> in the archive provided by <var>reader</var>.
     */
    public static void list(IHDF5Reader reader, ListParameters params, ListEntryVisitor visitor,
            boolean continueOnError, byte[] buffer)
    {
        params.check();
        final String objectPath = params.getFileOrDirectoryInArchive();
        final boolean isDirectory = reader.isGroup(objectPath, false);
        if (params.getStrategy().doExclude(objectPath, isDirectory))
        {
            return;
        }
        if (isDirectory)
        {
            list(reader, visitor, new IdCache(), objectPath, params, continueOnError, buffer);
        } else
        {
            final String dir = FilenameUtils.getFullPathNoEndSeparator(objectPath);
            final Link linkOrNull =
                    new DirectoryIndex(reader, "".equals(dir) ? "/" : dir, continueOnError,
                            params.isVerbose()).tryGetLink(FilenameUtils.getName(objectPath));
            if (linkOrNull == null)
            {
                dealWithError(new ListArchiveException(objectPath, "Object not found in archive."),
                        continueOnError);
                return;
            }
            try
            {
                process(reader, visitor, new IdCache(), objectPath, params, linkOrNull, buffer);
            } catch (IOException ex)
            {
                final File f = new File(objectPath);
                dealWithError(new ListArchiveException(f, ex), continueOnError);
            } catch (HDF5Exception ex)
            {
                dealWithError(new ListArchiveException(objectPath, ex), continueOnError);
            }
        }
    }

    /**
     * Adds the entries of <var>dir</var> to <var>entries</var> recursively.
     */
    private static void list(IHDF5Reader reader, ListEntryVisitor visitor, IdCache idCache,
            String dir, ListParameters params, boolean continueOnError, byte[] buffer)
    {
        if (reader.exists(dir, false) == false)
        {
            dealWithError(new ListArchiveException(dir, "Directory not found in archive."),
                    continueOnError);
            return;
        }
        final String dirPrefix = dir.endsWith("/") ? dir : (dir + "/");
        String path = "UNKNOWN";
        for (Link link : new DirectoryIndex(reader, dir, continueOnError, params.isVerbose()))
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
                    process(reader, visitor, idCache, path, params, link, buffer);
                }
                if (params.isRecursive() && link.isDirectory() && "/.".equals(path) == false)
                {
                    list(reader, visitor, idCache, path, params, continueOnError, buffer);
                }
            } catch (IOException ex)
            {
                final File f = new File(path);
                dealWithError(new ListArchiveException(f, ex), continueOnError);
            } catch (HDF5Exception ex)
            {
                dealWithError(new ListArchiveException(path, ex), continueOnError);
            }
        }
    }

    private static void process(IHDF5Reader reader, ListEntryVisitor visitor, IdCache idCache,
            String path, ListParameters params, Link link, byte[] buffer) throws IOException
    {
        final String errorLineOrNull = doCheck(reader, path, idCache, params, link, buffer);
        visitor.visit(new ListEntry(describeLink(path, link, idCache, params.isVerbose(),
                params.isNumeric()), errorLineOrNull));
    }

    private static String doCheck(IHDF5Reader reader, String path, IdCache idCache,
            ListParameters params, Link link, byte[] buffer) throws IOException
    {
        if (VERIFY_FS.contains(params.getCheck()))
        {
            return doFileSystemCheck(path, idCache, params, link, buffer);
        } else if (Check.CHECK_CRC_ARCHIVE == params.getCheck())
        {
            return doArchiveCheck(reader, path, params, link, buffer);
        }
        return null;
    }

    private static String doArchiveCheck(IHDF5Reader reader, String path, ListParameters params,
            Link link, byte[] buffer)
    {
        if (link.isRegularFile() == false)
        {
            return null;
        }
        final long size = reader.getSize(path);
        if (link.getSize() != size)
        {
            return "Archive file " + path + " failed size test, expected: " + link.getSize()
                    + ", found: " + size;
        }
        if (link.getSize() != 0 && link.getCrc32() == 0)
        {
            return "Archive file " + path + ": cannot verify (missing CRC checksum).";
        }
        final int crc32 = calcCRC32Archive(reader, path, link.getSize(), buffer);
        if (link.getCrc32() != crc32)
        {
            return "Archive file " + path + " failed CRC checksum test, expected: "
                    + hashToString(link.getCrc32()) + ", found: " + hashToString(crc32);
        }
        return null;
    }

    private static int calcCRC32Archive(IHDF5Reader reader, String objectPath, long size,
            byte[] buffer)
    {
        final CRC32 crc32Digest = new CRC32();
        long offset = 0;
        while (offset < size)
        {
            final int n =
                    reader.readAsByteArrayToBlockWithOffset(objectPath, buffer, buffer.length,
                            offset, 0);
            offset += n;
            crc32Digest.update(buffer, 0, n);
        }
        return (int) crc32Digest.getValue();
    }

    private static String doFileSystemCheck(String path, IdCache idCache, ListParameters params,
            Link link, byte[] buffer) throws IOException
    {
        final File f = new File(params.getFileOrDirectoryOnFileSystem(), path);
        if (f.exists() == false)
        {
            return "Object " + path + " does not exist on file system.";
        }
        final String symbolicLinkOrNull = tryGetSymbolicLink(f);
        if (symbolicLinkOrNull != null)
        {
            if (link.isSymLink() == false)
            {
                return "Object " + path + " is a " + link.getLinkType()
                        + " in archive, but a symlink on file system.";
            }
            if (symbolicLinkOrNull.equals(link.tryGetLinkTarget()) == false)
            {
                return "Symlink " + path + " links to " + link.tryGetLinkTarget()
                        + " in archive, but to " + symbolicLinkOrNull + " on file system";
            }
        } else if (f.isDirectory())
        {
            if (link.isDirectory() == false)
            {
                if (Unix.isOperational() || OSUtilities.isWindows())
                {
                    return "Object " + path + " is a " + link.getLinkType()
                            + " in archive, but a directory on file system.";
                } else
                {
                    return "Object " + path + " is a " + link.getLinkType()
                            + " in archive, but a directory on file system (error may be "
                            + "inaccurate because Unix system calls are not available.)";
                }
            }
        } else
        {
            if (link.isDirectory())
            {
                return "Object " + path + " is a directory in archive, but a file on file system.";

            }
            if (link.isSymLink())
            {
                if (Unix.isOperational() || OSUtilities.isWindows())
                {
                    return "Object " + path
                            + " is a symbolic link in archive, but a file on file system.";
                } else
                {
                    return "Object "
                            + path
                            + " is a symbolic link in archive, but a file on file system "
                            + "(error may be inaccurate because Unix system calls are not available.).";
                }

            }
            final long size = f.length();
            if (link.getSize() != size)
            {
                return "File " + f.getAbsolutePath() + " failed size test, expected: "
                        + link.getSize() + ", found: " + size;
            }
            if (link.getSize() > 0 && link.getCrc32() == 0)
            {
                return "File " + f.getAbsolutePath() + ": cannot verify (missing CRC checksum).";
            }
            final int crc32 = calcCRC32FileSystem(f, buffer);
            if (link.getCrc32() != crc32)
            {
                return "File " + f.getAbsolutePath() + " failed CRC checksum test, expected: "
                        + hashToString(link.getCrc32()) + ", found: " + hashToString(crc32) + ".";
            }
        }
        if (Check.VERIFY_CRC_ATTR_FS == params.check)
        {
            return doFileSystemAttributeCheck(f, idCache, link, params.isNumeric());
        }
        return null;
    }

    private static String tryGetSymbolicLink(File f)
    {
        if (Unix.isOperational())
        {
            return Unix.getFileInfo(f.getPath()).tryGetSymbolicLink();
        } else
        {
            return null;
        }
    }

    private static int calcCRC32FileSystem(File source, byte[] buffer) throws IOException
    {
        final InputStream input = FileUtils.openInputStream(source);
        final CRC32 crc32 = new CRC32();
        try
        {
            int n = 0;
            while (-1 != (n = input.read(buffer)))
            {
                crc32.update(buffer, 0, n);
            }
        } finally
        {
            IOUtils.closeQuietly(input);
        }
        return (int) crc32.getValue();
    }

    private static String doFileSystemAttributeCheck(File file, IdCache idCache, Link link,
            boolean numeric)
    {
        final StringBuilder sb = new StringBuilder();
        if (link.hasLastModified())
        {
            final long expectedLastModifiedMillis = link.getLastModified() * 1000L;
            final long foundLastModifiedMillis = file.lastModified();
            if (expectedLastModifiedMillis != foundLastModifiedMillis)
            {
                sb.append(String.format("'last modified time': (expected: "
                        + "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS, found: "
                        + "%2$tY-%2$tm-%2$td %2$tH:%2$tM:%2$tS) ", link.getLastModified(),
                        foundLastModifiedMillis));
            }
        }
        if (link.hasUnixPermissions() && Unix.isOperational())
        {
            final Stat info = Unix.getLinkInfo(file.getPath(), false);
            if (link.getPermissions() != info.getPermissions())
            {
                sb.append(String.format("'access permissions': (expected: %s, found: %s) ",
                        getPermissionString(link, numeric), getPermissionString(link, numeric)));
            }
            if (link.getUid() != info.getUid() || link.getGid() != info.getGid())
            {
                sb.append(String.format("'ownerwhip': (expected: %s:%s, found: %s:%s",
                        idCache.getUser(link, numeric), idCache.getGroup(link, numeric),
                        idCache.getUser(info, numeric), idCache.getGroup(info, numeric)));
            }
        }
        if (sb.length() == 0)
        {
            return null;
        } else
        {
            return sb.toString();
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

    private static int copyToHDF5(File source, final IHDF5Writer writer, final String objectPath,
            final long size, final HDF5GenericStorageFeatures compression, byte[] buffer)
            throws IOException
    {
        final InputStream input = FileUtils.openInputStream(source);
        final int blockSize = (int) Math.min(size, buffer.length);
        final HDF5OpaqueType type;
        if (writer.exists(objectPath, false))
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
            File destination, byte[] buffer) throws IOException
    {
        final OutputStream output = FileUtils.openOutputStream(destination);
        final CRC32 crc32 = new CRC32();
        try
        {
            long offset = 0;
            while (offset < size)
            {
                final int n =
                        reader.readAsByteArrayToBlockWithOffset(objectPath, buffer, buffer.length,
                                offset, 0);
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

    private static final char[] HEX_CHARACTERS =
        { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', };

    static String hashToString(final int checksum)
    {
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
