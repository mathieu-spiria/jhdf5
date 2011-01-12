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

import ch.rinn.restrictions.Private;
import ch.systemsx.cisd.base.unix.FileLinkType;
import ch.systemsx.cisd.base.unix.Unix;

/**
 * One entry of a archive listing.
 * 
 * @author Bernd Rinn
 */
public final class ListEntry
{
    static final long MILLIS_PER_SECOND = 1000L;

    private final String path;

    private final Link.Completeness completeness;

    private final String linkTarget;

    private final FileLinkType linkType;

    private final long size;

    private final long lastModified;

    private final int crc32;

    private final String user;

    private final String group;

    private final String permissions;

    private final boolean verbose;

    private final String errorLineOrNull;

    ListEntry(String path, Link link, IdCache idCache, boolean verbose, boolean numeric)
    {
        this.path = path;
        this.completeness = link.getCompleteness();
        this.linkTarget = link.tryGetLinkTarget() == null ? "?" : link.tryGetLinkTarget();
        this.linkType = link.getLinkType();
        this.size = link.getSize();
        this.lastModified = link.getLastModified();
        this.crc32 = link.getCrc32();
        this.user = idCache.getUser(link, numeric);
        this.group = idCache.getGroup(link, numeric);
        this.permissions = ListEntry.getPermissionString(link, numeric);
        this.verbose = verbose;
        this.errorLineOrNull = null;
    }

    ListEntry(String errorLineOrNull)
    {
        this.errorLineOrNull = errorLineOrNull;
        this.path = null;
        this.completeness = null;
        this.linkTarget = null;
        this.linkType = null;
        this.size = -1;
        this.lastModified = -1;
        this.crc32 = 0;
        this.user = null;
        this.group = null;
        this.permissions = null;
        this.verbose = false;
    }

    public String getPath()
    {
        return path;
    }

    public Link.Completeness getCompleteness()
    {
        return completeness;
    }

    public String getLinkTarget()
    {
        return linkTarget;
    }

    public FileLinkType getLinkType()
    {
        return linkType;
    }

    public long getSize()
    {
        return size;
    }

    public long getLastModified()
    {
        return lastModified;
    }

    public int getCrc32()
    {
        return crc32;
    }

    public String getUser()
    {
        return user;
    }

    public String getGroup()
    {
        return group;
    }

    public String getPermissions()
    {
        return permissions;
    }

    public boolean isVerbose()
    {
        return verbose;
    }

    public String getErrorLineOrNull()
    {
        return errorLineOrNull;
    }

    public boolean checkOK()
    {
        return errorLineOrNull == null;
    }

    public String describeLink()
    {
        if (verbose == false)
        {
            return path;
        }
        switch (completeness)
        {
            case BASE:
                if (linkType == FileLinkType.SYMLINK)
                {
                    return String.format("          \t%s -> %s", path, linkTarget);
                } else if (linkType == FileLinkType.DIRECTORY)
                {
                    return String.format("       DIR\t%s", path);
                } else
                {
                    return String.format("%10d\t%s\t%s%s", size,
                            ListEntry.hashToString(crc32), path,
                            (linkType == FileLinkType.REGULAR_FILE) ? "" : "\t*");
                }
            case LAST_MODIFIED:
                if (linkType == FileLinkType.SYMLINK)
                {
                    return String.format(
                            "          \t%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS\t%2$s -> %3$s",
                            lastModified * ListEntry.MILLIS_PER_SECOND, path, linkTarget);
                } else if (linkType == FileLinkType.DIRECTORY)
                {
                    return String.format("       DIR\t%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS\t%2$s",
                            lastModified * ListEntry.MILLIS_PER_SECOND, path);
                } else
                {
                    return String.format(
                            "%10d\t%2$tY-%2$tm-%2$td %2$tH:%2$tM:%2$tS\t%3$s\t%4$s%5$s", size,
                            lastModified * ListEntry.MILLIS_PER_SECOND,
                            ListEntry.hashToString(crc32), path,
                            (linkType == FileLinkType.REGULAR_FILE) ? "" : "\t*");
                }
            case FULL:
                if (linkType == FileLinkType.SYMLINK)
                {
                    return String
                            .format("%s\t%s\t%s\t          \t%4$tY-%4$tm-%4$td %4$tH:%4$tM:%4$tS\t00000000\t%5$s -> %6$s",
                                    permissions, user, group, lastModified
                                            * ListEntry.MILLIS_PER_SECOND, path, linkTarget);
                } else if (linkType == FileLinkType.DIRECTORY)
                {
                    return String
                            .format("%s\t%s\t%s\t       DIR\t%4$tY-%4$tm-%4$td %4$tH:%4$tM:%4$tS\t        \t%5$s",
                                    permissions, user, group, lastModified
                                            * ListEntry.MILLIS_PER_SECOND, path);
                } else
                {
                    return String
                            .format("%s\t%s\t%s\t%10d\t%5$tY-%5$tm-%5$td %5$tH:%5$tM:%5$tS\t%6$s\t%7$s%8$s",
                                    permissions, user, group, size, lastModified
                                            * ListEntry.MILLIS_PER_SECOND,
                                    ListEntry.hashToString(crc32), path,
                                    (linkType == FileLinkType.REGULAR_FILE) ? "" : "\t*");
                }
            default:
                throw new Error("Unknown level of link information completeness: " + completeness);
        }
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
}