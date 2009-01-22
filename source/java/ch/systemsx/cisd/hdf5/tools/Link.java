/*
 * Copyright 2009 ETH Zuerich, CISD
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
import ch.systemsx.cisd.common.os.FileLinkType;
import ch.systemsx.cisd.hdf5.HDF5LinkInformation;

/**
 * A class containing all information we need have about a link either in the file sysstem or in an
 * HDF5 container.
 * 
 * @author Bernd Rinn
 */
public final class Link implements Comparable<Link>
{
    public enum Completeness
    {
        BASE, LAST_MODIFIED, FULL
    }

    private final String linkName;

    private final String linkTargetOrNull;

    private final FileLinkType linkType;

    private final long size;

    private final long lastModified;

    private final int uid;

    private final int gid;

    private final short permissions;

    public Link(LinkInfo info, String linkTargetOrNull)
    {
        this.linkName = info.getLinkName();
        this.linkTargetOrNull = linkTargetOrNull;
        this.linkType = info.getLinkType();
        this.size = info.getSize();
        this.lastModified = info.getLastModified();
        this.uid = info.getUid();
        this.gid = info.getGid();
        this.permissions = info.getPermissions();
    }

    public Link(HDF5LinkInformation info, long size, long lastModified)
    {
        this.linkName = info.getName();
        this.linkTargetOrNull = info.tryGetSymbolicLinkTarget();
        this.linkType = HDF5ArchiveTools.translateType(info.getType());
        this.size = size;
        this.lastModified = lastModified;
        this.uid = LinkInfo.UNKNOWN;
        this.gid = LinkInfo.UNKNOWN;
        this.permissions = LinkInfo.UNKNOWN_S;
    }

    /** For unit tests only! */
    @Private
    Link(String linkName, String linkTargetOrNull, FileLinkType linkType, long size,
            long lastModified, int uid, int gid, short permissions)
    {
        this.linkName = linkName;
        this.linkTargetOrNull = linkTargetOrNull;
        this.linkType = linkType;
        this.size = size;
        this.lastModified = lastModified;
        this.uid = uid;
        this.gid = gid;
        this.permissions = permissions;
    }

    public String getLinkName()
    {
        return linkName;
    }

    public String tryGetLinkTarget()
    {
        return linkTargetOrNull;
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

    public long getSize()
    {
        return size;
    }

    public boolean hasLastModified()
    {
        return lastModified >= 0;
    }

    public long getLastModified()
    {
        return lastModified;
    }

    public boolean hasUnixPermissions()
    {
        return uid >= 0 && gid >= 0 && permissions >= 0;
    }

    public int getUid()
    {
        return uid;
    }

    public int getGid()
    {
        return gid;
    }

    public short getPermissions()
    {
        return permissions;
    }

    public Completeness getCompleteness()
    {
        if (hasUnixPermissions())
        {
            return Completeness.FULL;
        } else if (hasLastModified())
        {
            return Completeness.LAST_MODIFIED;
        } else
        {
            return Completeness.BASE;
        }
    }

    //
    // Comparable
    //

    public int compareTo(Link o)
    {
        if (isDirectory() && o.isDirectory() == false)
        {
            return -1;
        } else if (isDirectory() == false && o.isDirectory())
        {
            return 1;
        } else
        {
            return getLinkName().compareTo(o.getLinkName());
        }
    }
}
