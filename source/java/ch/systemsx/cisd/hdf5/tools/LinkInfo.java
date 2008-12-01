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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import ch.systemsx.cisd.common.exceptions.CheckedExceptionTunnel;
import ch.systemsx.cisd.common.os.Unix;
import ch.systemsx.cisd.common.os.FileLinkType;
import ch.systemsx.cisd.common.os.Unix.Stat;

/**
 * A class containing all information we need to store about a link.
 * 
 * @author Bernd Rinn
 */
public final class LinkInfo implements Serializable, Comparable<LinkInfo>
{
    private static final long serialVersionUID = 1L;

    private final static int UNKNOWN = -1;

    private final static short UNKNOWN_S = -1;

    private final String linkName;

    private final FileLinkType linkType;

    private final long size;

    private final long lastModified;

    private final int uid;

    private final int gid;

    private final short permissions;

    LinkInfo(String linkName, FileLinkType linkType, long size, long lastModified, int uid,
            int gid, short permissions)
    {
        this.linkName = linkName;
        this.linkType = linkType;
        this.size = size;
        this.lastModified = lastModified;
        this.uid = uid;
        this.gid = gid;
        this.permissions = permissions;
    }

    LinkInfo(String linkName, FileLinkType linkType, long size, long lastModified)
    {
        this(linkName, linkType, size, lastModified, UNKNOWN, UNKNOWN, UNKNOWN_S);
    }

    /**
     * Returns the {@link LinkInfo} for the given <var>link</var>.
     */
    public static LinkInfo get(File link, boolean includeOwnerAndPermissions)
    {
        if (includeOwnerAndPermissions && Unix.isOperational())
        {
            final Stat info = Unix.getLinkInfo(link.getPath(), false);
            return new LinkInfo(link.getName(), info.getLinkType(), info.getSize(), info
                    .getLastModified(), info.getUid(), info.getGid(), info.getPermissions());
        } else
        {
            final FileLinkType linkType =
                    (link.isDirectory()) ? FileLinkType.DIRECTORY
                            : (link.isFile() ? FileLinkType.REGULAR_FILE : FileLinkType.OTHER);
            return new LinkInfo(link.getName(), linkType, link.length(), link.lastModified() / 1000);
        }
    }

    /**
     * Returns the link target of <var>symbolicLink</var>, or <code>null</code>, if
     * <var>symbolicLink</var> is not a symbolic link or the link target could not be read.
     */
    public static String tryReadLinkTarget(File symbolicLink)
    {
        if (Unix.isOperational())
        {
            return Unix.tryReadSymbolicLink(symbolicLink.getPath());
        } else
        {
            return null;
        }
    }

    /**
     * Returns the storage form (a byte array) of an array of {@link LinkInfo} objects.
     */
    public static byte[] toStorageForm(LinkInfo[] infoArray)
    {
        final ByteArrayOutputStream ba = new ByteArrayOutputStream();
        try
        {
            final ObjectOutputStream oo = new ObjectOutputStream(ba);
            oo.writeObject(infoArray);
            oo.close();
            return ba.toByteArray();
        } catch (Exception ex)
        {
            throw CheckedExceptionTunnel.wrapIfNecessary(ex);
        }
    }

    /**
     * Returns an array of {@link LinkInfo} objects from its storage form (a byte array).
     */
    public static LinkInfo[] fromStorageForm(byte[] storageForm)
    {
        final ByteArrayInputStream bi = new ByteArrayInputStream(storageForm);
        try
        {
            final ObjectInputStream oi = new ObjectInputStream(bi);
            return (LinkInfo[]) oi.readObject();
        } catch (Exception ex)
        {
            throw CheckedExceptionTunnel.wrapIfNecessary(ex);
        }
    }

    public String getLinkName()
    {
        return linkName;
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

    public long getSize()
    {
        return size;
    }

    public long getLastModified()
    {
        return lastModified;
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

    //
    // Comparable
    //

    public int compareTo(LinkInfo o)
    {
        if (isDirectory() && o.isDirectory() == false)
        {
            return 1;
        } else if (isDirectory() == false && o.isDirectory())
        {
            return -1;
        } else
        {
            return getLinkName().compareTo(o.getLinkName());
        }
    }
}
