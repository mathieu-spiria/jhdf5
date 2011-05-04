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
import java.io.InputStream;
import java.util.zip.CRC32;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import ch.systemsx.cisd.base.unix.Unix;
import ch.systemsx.cisd.base.unix.Unix.Stat;
import ch.systemsx.cisd.base.utilities.OSUtilities;

/**
 * A helper class that checks a particular link from an <code>h5ar</code> archive with a particular
 * path on the file system.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiveLinkChecker
{

    static String checkLink(Link link, String path, ListParameters params, IdCache idCache,
            byte[] buffer) throws IOException
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
                        + ListEntry.hashToString(link.getCrc32()) + ", found: "
                        + ListEntry.hashToString(crc32) + ".";
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
                        ListEntry.getPermissionString(link, numeric),
                        ListEntry.getPermissionString(link, numeric)));
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

}
