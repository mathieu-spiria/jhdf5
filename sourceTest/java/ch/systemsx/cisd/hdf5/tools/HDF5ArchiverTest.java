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
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.List;

import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import ch.rinn.restrictions.Friend;
import ch.systemsx.cisd.base.unix.FileLinkType;
import ch.systemsx.cisd.base.unix.Unix;
import ch.systemsx.cisd.base.utilities.OSUtilities;

/**
 * Tests for {@link HDF5Archiver}.
 * 
 * @author Bernd Rinn
 */
@Friend(toClasses =
    { HDF5Archiver.class, IdCache.class, Link.class })
public class HDF5ArchiverTest
{
    private static final File rootDirectory = new File("targets", "unit-test-wd");

    private static final File workingDirectory = new File(rootDirectory, "hdf5-archivertest-wd");

    @BeforeSuite
    public void init()
    {
        workingDirectory.mkdirs();
        assertTrue(workingDirectory.isDirectory());
        workingDirectory.deleteOnExit();
        rootDirectory.deleteOnExit();
    }

    @Override
    protected void finalize() throws Throwable
    {
        // Delete the working directory
        if (workingDirectory.exists() && workingDirectory.canWrite())
        {
            workingDirectory.delete();
        }
        // Delete root directory
        if (rootDirectory.exists() && rootDirectory.canWrite())
        {
            rootDirectory.delete();
        }

        super.finalize();
    }

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
        assertEquals("----------", ListEntry.getPermissionString(createPerms(0), false));
        assertEquals("-rw-rw-rw-", ListEntry.getPermissionString(createPerms(0666), false));
        assertEquals("-r--r--r--", ListEntry.getPermissionString(createPerms(0444), false));
        assertEquals("-rwx------", ListEntry.getPermissionString(createPerms(0700), false));
        assertEquals("-rwsr-xr-x", ListEntry.getPermissionString(createPerms(04755), false));
        assertEquals("-rwSr-xr-x", ListEntry.getPermissionString(createPerms(04655), false));
        assertEquals("-rwxr-sr-x", ListEntry.getPermissionString(createPerms(02755), false));
        assertEquals("-rwxr-Sr-x", ListEntry.getPermissionString(createPerms(02745), false));
        assertEquals("-rwxr-xr-t", ListEntry.getPermissionString(createPerms(01755), false));
        assertEquals("-rwxr-xr-T", ListEntry.getPermissionString(createPerms(01754), false));
        assertEquals("d---------", ListEntry.getPermissionString(createDirPerms(0), false));
        assertEquals("drwxr-xr-x", ListEntry.getPermissionString(createDirPerms(0755), false));
    }

    @Test(groups =
        { "requires_unix" })
    public void testDescribeLink()
    {
        final String rootGroupName = OSUtilities.isMacOS() ? "wheel" : "root";
        final IdCache idCache = new IdCache();
        assertEquals("dir/link_name", new ListEntry("dir/link_name", new Link(null, null, null, -1,
                -1, -1, -1, (short) -1), idCache, false, false).describeLink());
        assertEquals("       100\t00000000\tdir/link_name", new ListEntry("dir/link_name",
                new Link(null, null, FileLinkType.REGULAR_FILE, 100, -1, -1, -1, (short) -1),
                idCache, true, false).describeLink());
        assertEquals("-rwxr-xr-x\troot\t" + rootGroupName
                + "\t       111\t2000-01-01 00:00:00\t00000000\tdir/link_name",
                new ListEntry("dir/link_name", new Link(null, null, FileLinkType.REGULAR_FILE,
                        111L, 946681200491L / 1000L, 0, 0, (short) 0755), idCache, true, false)
                        .describeLink());
        assertEquals("d---------\troot\t" + rootGroupName
                + "\t       DIR\t2000-01-01 00:00:00\t        \tdir/link_name",
                new ListEntry("dir/link_name", new Link(null, null, FileLinkType.DIRECTORY, 111L,
                        946681200491L / 1000L, 0, 0, (short) 0, 0), idCache, true, false)
                        .describeLink());
        assertEquals("755\t0\t0\t       111\t2000-01-01 00:00:00\t" + ListEntry.hashToString(200)
                + "\tdir/link_name", new ListEntry("dir/link_name", new Link(null, null,
                FileLinkType.REGULAR_FILE, 111L, 946681200491L / 1000L, 0, 0, (short) 0755, 200),
                idCache, true, true).describeLink());
        assertEquals("0\t0\t0\t       DIR\t2000-01-01 00:00:00\t        \tdir/link_name",
                new ListEntry("dir/link_name", new Link("link_name2", null, FileLinkType.DIRECTORY,
                        111L, 946681200491L / 1000L, 0, 0, (short) 0), idCache, true, true)
                        .describeLink());
        assertEquals("       111\t2000-01-01 00:00:00\t00000000\tdir/link_name",
                new ListEntry("dir/link_name",
                        new Link("link_name", null, FileLinkType.REGULAR_FILE, 111L,
                                946681200491L / 1000L, -1, 0, (short) 0755), idCache, true, false)
                        .describeLink());
        assertEquals("       111\t00000000\tdir/link_name", new ListEntry("dir/link_name",
                new Link("link_name2", null, FileLinkType.REGULAR_FILE, 111L, -1L, -1, 0,
                        (short) 0755), idCache, true, false).describeLink());
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
        final IdCache idCache = new IdCache();
        assertEquals("-17",
                idCache.getUser(new Link(null, null, null, -1, -1, -17, -1, (short) -1), true));
        assertEquals("root",
                idCache.getUser(new Link(null, null, null, -1, -1, 0, -1, (short) -1), false));
        assertEquals(uname,
                idCache.getUser(new Link(null, null, null, -1, -1, uid, -1, (short) -1), false));
        int invalidUid;
        for (invalidUid = 60000; invalidUid < 65535 && Unix.tryGetUserByUid(invalidUid) != null; ++invalidUid)
        {
        }
        assertEquals(Integer.toString(invalidUid), idCache.getUser(new Link(null, null, null, -1,
                -1, invalidUid, -1, (short) -1), false));
    }

    private void writeToArchive(final HDF5Archiver a, final String name, final String content)
    {
        final byte[] bytes = content.getBytes();
        a.archiveToFile("/test", new Link(name, null, FileLinkType.REGULAR_FILE, bytes.length,
                1000000L, 100, 100, (short) 0755), new ByteArrayInputStream(bytes), null);
    }

    @Test
    public void testWriteToFile()
    {
        final File file = new File(workingDirectory, "testarchive.h5ar");
        file.delete();
        file.deleteOnExit();
        final HDF5Archiver a = new HDF5Archiver(file, false);
        writeToArchive(a, "hello.txt", "Hello World\n");
        writeToArchive(a, "hello2.txt", "Yet another Hello World\n");
        a.close();
        final HDF5Archiver aro = new HDF5Archiver(file, true);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        aro.cat("/test/hello.txt", out);
        assertEquals("Hello World\n", new String(out.toByteArray()));
        out.reset();
        aro.cat("/test/hello2.txt", out);
        assertEquals("Yet another Hello World\n", new String(out.toByteArray()));
        final List<ListEntry> list =
                aro.list("/", null, false, false, true, true, Check.CHECK_CRC_ARCHIVE);
        assertEquals(1, list.size());
        assertEquals("755\t100\t100\t       DIR\t1970-01-12 14:46:40\t        \t/test", list.get(0)
                .describeLink());
        final List<ListEntry> list2 =
                aro.list("/test", null, false, false, true, true, Check.CHECK_CRC_ARCHIVE);
        assertEquals(2, list2.size());
        assertEquals("755\t100\t100\t        12\t1970-01-12 14:46:40\tb095e5e3\t/test/hello.txt",
                list2.get(0).describeLink());
        assertEquals("755\t100\t100\t        24\t1970-01-12 14:46:40\tee5f3107\t/test/hello2.txt",
                list2.get(1).describeLink());
        aro.close();
    }

}
