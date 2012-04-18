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

import ch.systemsx.cisd.base.unix.FileLinkType;

/**
 * An entry of an archive listing.
 * 
 * @author Bernd Rinn
 */
public final class ArchiveEntry
{
    private final String path;

    private final String parentPath;

    private final String name;

    private final ArchiveEntryCompleteness completeness;

    private final boolean hasLinkTarget;
    
    private final String linkTarget;

    private final FileLinkType linkType;

    private final FileLinkType verifiedLinkType;

    private final long size;

    private final long verifiedSize;

    private final long lastModified;

    private final int crc32;

    private final int verifiedCrc32;

    private final int uid;

    private final int gid;

    private final IdCache idCache;

    private final short permissions;

    private final String errorLineOrNull;

    ArchiveEntry(String dir, String path, LinkRecord link, IdCache idCache)
    {
        this(dir, path, link, idCache, null);
    }
    
    ArchiveEntry(String dir, String path, LinkRecord link, IdCache idCache, String errorLineOrNull)
    {
        this.parentPath = dir;
        this.path = path;
        this.name = link.getLinkName();
        this.idCache = idCache;
        this.completeness = link.getCompleteness();
        this.hasLinkTarget = (link.tryGetLinkTarget() != null);
        this.linkTarget = hasLinkTarget ? link.tryGetLinkTarget() : "?";
        this.linkType = link.getLinkType();
        this.verifiedLinkType = link.getVerifiedType();
        this.size = link.getSize();
        this.verifiedSize = link.getVerifiedSize();
        this.lastModified = link.getLastModified();
        this.crc32 = link.getCrc32();
        this.verifiedCrc32 = link.getVerifiedCrc32();
        this.uid = link.getUid();
        this.gid = link.getGid();
        this.permissions = link.getPermissions();
        this.errorLineOrNull = errorLineOrNull;
    }

    ArchiveEntry(ArchiveEntry pathInfo, ArchiveEntry linkInfo)
    {
        this.parentPath = pathInfo.parentPath;
        this.path = pathInfo.path;
        this.name = pathInfo.name;
        this.idCache = pathInfo.idCache;
        this.completeness = linkInfo.completeness;
        this.hasLinkTarget = linkInfo.hasLinkTarget;
        this.linkTarget = linkInfo.linkTarget;
        this.linkType = linkInfo.linkType;
        this.verifiedLinkType = linkInfo.verifiedLinkType;
        this.size = linkInfo.size;
        this.verifiedSize = linkInfo.verifiedSize;
        this.lastModified = Math.max(pathInfo.lastModified, linkInfo.lastModified);
        this.crc32 = linkInfo.crc32;
        this.verifiedCrc32 = linkInfo.verifiedCrc32;
        this.uid = linkInfo.uid;
        this.gid = linkInfo.gid;
        this.permissions = linkInfo.permissions;
        this.errorLineOrNull = null;
    }

    ArchiveEntry(String errorLineOrNull)
    {
        this.errorLineOrNull = errorLineOrNull;
        this.path = null;
        this.parentPath = null;
        this.name = null;
        this.idCache = null;
        this.completeness = null;
        this.linkTarget = null;
        this.hasLinkTarget = false;
        this.linkType = null;
        this.verifiedLinkType = null;
        this.size = -1;
        this.verifiedSize = -1;
        this.lastModified = -1;
        this.crc32 = 0;
        this.verifiedCrc32 = 0;
        this.uid = 0;
        this.gid = 0;
        this.permissions = 0;
    }

    public String getPath()
    {
        return path;
    }

    public String getParentPath()
    {
        return parentPath;
    }

    public String getName()
    {
        return name;
    }

    public ArchiveEntryCompleteness getCompleteness()
    {
        return completeness;
    }

    public String getLinkTarget()
    {
        return linkTarget;
    }

    public boolean hasLinkTarget()
    {
        return hasLinkTarget;
    }

    public FileLinkType getLinkType()
    {
        return linkType;
    }

    public boolean isDirectory()
    {
        return linkType == FileLinkType.DIRECTORY;
    }

    public boolean isSymLink()
    {
        return linkType == FileLinkType.SYMLINK;
    }

    public boolean isRegularFile()
    {
        return linkType == FileLinkType.REGULAR_FILE;
    }

    public FileLinkType tryGetVerifiedLinkType()
    {
        return verifiedLinkType;
    }

    public long getSize()
    {
        return size;
    }

    public long getVerifiedSize()
    {
        return verifiedSize;
    }

    public long getLastModified()
    {
        return lastModified;
    }

    public int getCrc32()
    {
        return crc32;
    }

    public String getCrc32Str()
    {
        return Utils.crc32ToString(crc32);
    }

    public int getVerifiedCrc32()
    {
        return verifiedCrc32;
    }

    public String getVerifiedCrc32Str()
    {
        return Utils.crc32ToString(verifiedCrc32);
    }

    public String getUser(boolean numeric)
    {
        return idCache.getUser(uid, numeric);
    }

    public int getUid()
    {
        return uid;
    }

    public String getGroup(boolean numeric)
    {
        return idCache.getGroup(gid, numeric);
    }

    public int getGid()
    {
        return gid;
    }

    public short getPermissions()
    {
        return permissions;
    }

    public String getPermissionsString(boolean numeric)
    {
        return Utils.permissionsToString(permissions, linkType == FileLinkType.DIRECTORY, numeric);
    }

    public String getErrorLineOrNull()
    {
        return errorLineOrNull;
    }

    public boolean hasCheck()
    {
        return (verifiedLinkType != null || verifiedSize != -1 || verifiedCrc32 != 0 || errorLineOrNull != null);
    }

    public boolean isOK()
    {
        return (errorLineOrNull == null) && linkTypeOK() && sizeOK() && checksumOK();
    }

    public boolean linkTypeOK()
    {
        return (verifiedLinkType == null) || (linkType == verifiedLinkType);
    }

    public boolean sizeOK()
    {
        return (verifiedSize == -1) || (size == verifiedSize);
    }

    public boolean checksumOK()
    {
        return (verifiedSize == -1) || (crc32 == verifiedCrc32);
    }

    public String getStatus(boolean verbose)
    {
        if (isOK() == false)
        {
            if (errorLineOrNull != null)
            {
                return "ERROR: " + errorLineOrNull;
            } else if (linkTypeOK() == false)
            {
                return verbose ? String.format("ERROR: link type mismatch [expected: %s, found: %s]",
                        linkType, verifiedLinkType) : "WRONG TYPE";
            } else if (sizeOK() == false)
            {
                return verbose ? String.format("ERROR: size mismatch [expected: %d, found: %d]",
                        size, verifiedSize) : "WRONG SIZE";
            } else if (checksumOK() == false)
            {
                return verbose ? String.format(
                        "ERROR: checksum mismatch [expected: %s, found: %s]",
                        Utils.crc32ToString(crc32), Utils.crc32ToString(verifiedCrc32))
                        : "WRONG CRC32";
            }
        }
        return "OK";
    }

    public String describeLink()
    {
        return describeLink(true, false, true);
    }
    
    public String describeLink(boolean verbose)
    {
        return describeLink(verbose, false, true);
    }
    
    public String describeLink(boolean verbose, boolean numeric)
    {
        return describeLink(verbose, numeric, true);
    }
    
    public String describeLink(boolean verbose, boolean numeric, boolean includeCheck)
    {
        final StringBuilder builder = new StringBuilder();
        if (verbose == false)
        {
            builder.append(path);
        } else
        {
            switch (completeness)
            {
                case BASE:
                    if (linkType == FileLinkType.SYMLINK)
                    {
                        builder.append(String.format("          \t%s -> %s", path, linkTarget));
                    } else if (linkType == FileLinkType.DIRECTORY)
                    {
                        builder.append(String.format("       DIR\t%s", path));
                    } else
                    {
                        builder.append(String.format("%10d\t%s\t%s%s", size,
                                Utils.crc32ToString(crc32), path,
                                (linkType == FileLinkType.REGULAR_FILE) ? "" : "\t*"));
                    }
                    break;
                case LAST_MODIFIED:
                    if (linkType == FileLinkType.SYMLINK)
                    {
                        builder.append(String.format(
                                "          \t%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS\t%2$s -> %3$s",
                                lastModified * Utils.MILLIS_PER_SECOND, path, linkTarget));
                    } else if (linkType == FileLinkType.DIRECTORY)
                    {
                        builder.append(String.format(
                                "       DIR\t%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS\t%2$s",
                                lastModified * Utils.MILLIS_PER_SECOND, path));
                    } else
                    {
                        builder.append(String.format(
                                "%10d\t%2$tY-%2$tm-%2$td %2$tH:%2$tM:%2$tS\t%3$s\t%4$s%5$s", size,
                                lastModified * Utils.MILLIS_PER_SECOND, Utils.crc32ToString(crc32),
                                path, (linkType == FileLinkType.REGULAR_FILE) ? "" : "\t*"));
                    }
                    break;
                case FULL:
                    if (linkType == FileLinkType.SYMLINK)
                    {
                        builder.append(String
                                .format("%s\t%s\t%s\t          \t%4$tY-%4$tm-%4$td %4$tH:%4$tM:%4$tS\t        \t%5$s -> %6$s",
                                        Utils.permissionsToString(permissions, false, numeric),
                                        getUser(numeric), getGroup(numeric), lastModified
                                                * Utils.MILLIS_PER_SECOND, path, linkTarget));
                    } else if (linkType == FileLinkType.DIRECTORY)
                    {
                        builder.append(String
                                .format("%s\t%s\t%s\t       DIR\t%4$tY-%4$tm-%4$td %4$tH:%4$tM:%4$tS\t        \t%5$s",
                                        Utils.permissionsToString(permissions, true, numeric),
                                        getUser(numeric), getGroup(numeric), lastModified
                                                * Utils.MILLIS_PER_SECOND, path));
                    } else
                    {
                        builder.append(String
                                .format("%s\t%s\t%s\t%10d\t%5$tY-%5$tm-%5$td %5$tH:%5$tM:%5$tS\t%6$s\t%7$s%8$s",
                                        Utils.permissionsToString(permissions, false, numeric),
                                        getUser(numeric), getGroup(numeric), size, lastModified
                                                * Utils.MILLIS_PER_SECOND,
                                        Utils.crc32ToString(crc32), path,
                                        (linkType == FileLinkType.REGULAR_FILE) ? "" : "\t*"));
                    }
                    break;
                default:
                    throw new Error("Unknown level of link completeness: " + completeness);
            }
        }
        if (includeCheck && hasCheck())
        {
            builder.append('\t');
            builder.append(getStatus(false));
        }
        return builder.toString();
    }

    @Override
    public String toString()
    {
        return describeLink();
    }

}