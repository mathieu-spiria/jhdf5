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

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

import ch.rinn.restrictions.Friend;
import ch.systemsx.cisd.base.unix.FileLinkType;
import ch.systemsx.cisd.base.unix.Unix;
import ch.systemsx.cisd.base.utilities.OSUtilities;

/**
 * Tests for {@link HDF5ArchiveTools}.
 * 
 * @author Bernd Rinn
 */
@Friend(toClasses = HDF5ArchiveTools.class)
public class HDF5ArchiveToolsTest
{

    private static Link createPerms(int perms)
    {
        return new Link(null, null, null, -1, -1, -1, -1, (short) perms);
    }

    private static Link createDirPerms(int perms)
    {
        return new Link(null, null, FileLinkType.DIRECTORY, -1, -1, -1, -1, (short) perms);
    }

    @Test
    public void testGetPermissions()
    {
        assertEquals("----------", HDF5ArchiveTools.getPermissionString(createPerms(0), false));
        assertEquals("-rw-rw-rw-", HDF5ArchiveTools.getPermissionString(createPerms(0666), false));
        assertEquals("-r--r--r--", HDF5ArchiveTools.getPermissionString(createPerms(0444), false));
        assertEquals("-rwx------", HDF5ArchiveTools.getPermissionString(createPerms(0700), false));
        assertEquals("-rwsr-xr-x", HDF5ArchiveTools.getPermissionString(createPerms(04755), false));
        assertEquals("-rwSr-xr-x", HDF5ArchiveTools.getPermissionString(createPerms(04655), false));
        assertEquals("-rwxr-sr-x", HDF5ArchiveTools.getPermissionString(createPerms(02755), false));
        assertEquals("-rwxr-Sr-x", HDF5ArchiveTools.getPermissionString(createPerms(02745), false));
        assertEquals("-rwxr-xr-t", HDF5ArchiveTools.getPermissionString(createPerms(01755), false));
        assertEquals("-rwxr-xr-T", HDF5ArchiveTools.getPermissionString(createPerms(01754), false));
        assertEquals("d---------", HDF5ArchiveTools.getPermissionString(createDirPerms(0), false));
        assertEquals("drwxr-xr-x", HDF5ArchiveTools
                .getPermissionString(createDirPerms(0755), false));
    }

    @Test(groups =
        { "requires_unix" })
    public void testDescribeLink()
    {
        final String rootGroupName = OSUtilities.isMacOS() ? "wheel" : "root";
        final HDF5ArchiveTools.IdCache idCache = new HDF5ArchiveTools.IdCache();
        assertEquals("dir/link_name", HDF5ArchiveTools.describeLink("dir/link_name", new Link(null,
                null, null, -1, -1, -1, -1, (short) -1), idCache, false, false));
        assertEquals("       100\tdir/link_name", HDF5ArchiveTools.describeLink("dir/link_name",
                new Link(null, null, FileLinkType.REGULAR_FILE, 100, -1, -1, -1, (short) -1),
                idCache, true, false));
        assertEquals("-rwxr-xr-x\troot\t" + rootGroupName
                + "\t       111\t2000-01-01 00:00:00\t00000000\tdir/link_name", HDF5ArchiveTools
                .describeLink("dir/link_name", new Link(null, null, FileLinkType.REGULAR_FILE,
                        111L, 946681200491L / 1000L, 0, 0, (short) 0755), idCache, true, false));
        assertEquals("d---------\troot\t" + rootGroupName
                + "\t       DIR\t2000-01-01 00:00:00\t        \tdir/link_name", HDF5ArchiveTools
                .describeLink("dir/link_name", new Link(null, null, FileLinkType.DIRECTORY, 111L,
                        946681200491L / 1000L, 0, 0, (short) 0, 0), idCache, true, false));
        assertEquals("755\t0\t0\t       111\t2000-01-01 00:00:00\t"
                + HDF5ArchiveTools.hashToString(200) + "\tdir/link_name", HDF5ArchiveTools
                .describeLink("dir/link_name", new Link(null, null, FileLinkType.REGULAR_FILE,
                        111L, 946681200491L / 1000L, 0, 0, (short) 0755, 200), idCache, true, true));
        assertEquals("0\t0\t0\t       DIR\t2000-01-01 00:00:00\t        \tdir/link_name", HDF5ArchiveTools
                .describeLink("dir/link_name", new Link("link_name2", null, FileLinkType.DIRECTORY,
                        111L, 946681200491L / 1000L, 0, 0, (short) 0), idCache, true, true));
        assertEquals("       111\t2000-01-01 00:00:00\tdir/link_name", HDF5ArchiveTools
                .describeLink("dir/link_name",
                        new Link("link_name", null, FileLinkType.REGULAR_FILE, 111L,
                                946681200491L / 1000L, -1, 0, (short) 0755), idCache, true, false));
        assertEquals("       111\tdir/link_name", HDF5ArchiveTools.describeLink("dir/link_name",
                new Link("link_name2", null, FileLinkType.REGULAR_FILE, 111L, -1L, -1, 0,
                        (short) 0755), idCache, true, false));
    }

    @Test(groups =
        { "requires_unix" })
    public void testIdCache()
    {
        if (Unix.isOperational() == false)
        {
            return;
        }
        final int uid = Unix.getUid();
        final String uname = Unix.tryGetUserNameForUid(uid);
        final HDF5ArchiveTools.IdCache idCache = new HDF5ArchiveTools.IdCache();
        assertEquals("-17", idCache.getUser(
                new Link(null, null, null, -1, -1, -17, -1, (short) -1), true));
        assertEquals("root", idCache.getUser(new Link(null, null, null, -1, -1, 0, -1, (short) -1),
                false));
        assertEquals(uname, idCache.getUser(
                new Link(null, null, null, -1, -1, uid, -1, (short) -1), false));
        int invalidUid;
        for (invalidUid = 60000; invalidUid < 65535 && Unix.tryGetUserByUid(invalidUid) != null; ++invalidUid)
        {
        }
        assertEquals(Integer.toString(invalidUid), idCache.getUser(new Link(null, null, null, -1,
                -1, invalidUid, -1, (short) -1), false));
    }

}
