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

package ch.systemsx.cisd.hdf5.h5ar;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintStream;
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
    { HDF5Archiver.class, IdCache.class, LinkRecord.class })
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

    @Test
    public void testGetPermissions()
    {
        assertEquals("----------", Utils.permissionsToString(0, false, false));
        assertEquals("-rw-rw-rw-", Utils.permissionsToString(0666, false, false));
        assertEquals("-r--r--r--", Utils.permissionsToString(0444, false, false));
        assertEquals("-rwx------", Utils.permissionsToString(0700, false, false));
        assertEquals("-rwsr-xr-x", Utils.permissionsToString(04755, false, false));
        assertEquals("-rwSr-xr-x", Utils.permissionsToString(04655, false, false));
        assertEquals("-rwxr-sr-x", Utils.permissionsToString(02755, false, false));
        assertEquals("-rwxr-Sr-x", Utils.permissionsToString(02745, false, false));
        assertEquals("-rwxr-xr-t", Utils.permissionsToString(01755, false, false));
        assertEquals("-rwxr-xr-T", Utils.permissionsToString(01754, false, false));
        assertEquals("d---------", Utils.permissionsToString(0, true, false));
        assertEquals("drwxr-xr-x", Utils.permissionsToString(0755, true, false));
    }

    @Test(groups =
        { "requires_unix" })
    public void testDescribeLink()
    {
        final String rootGroupName = OSUtilities.isMacOS() ? "wheel" : "root";
        final IdCache idCache = new IdCache();
        assertEquals("dir/link_name", new ArchiveEntry("dir", "dir/link_name", new LinkRecord(null,
                null, null, -1, -1, -1, -1, (short) -1, 0), idCache).describeLink(false, false));
        assertEquals("       100\t00000000\tdir/link_name", new ArchiveEntry("dir",
                "dir/link_name", new LinkRecord(null, null, FileLinkType.REGULAR_FILE, 100, -1, -1,
                        -1, (short) -1, 0), idCache).describeLink(true, false));
        assertEquals("-rwxr-xr-x\troot\t" + rootGroupName
                + "\t       111\t2000-01-01 00:00:00\t00000000\tdir/link_name", new ArchiveEntry(
                "dir", "dir/link_name", new LinkRecord(null, null, FileLinkType.REGULAR_FILE, 111L,
                        946681200491L / 1000L, 0, 0, (short) 0755, 0), idCache).describeLink(true,
                false));
        assertEquals("d---------\troot\t" + rootGroupName
                + "\t       DIR\t2000-01-01 00:00:00\t        \tdir/link_name", new ArchiveEntry(
                "dir", "dir/link_name", new LinkRecord(null, null, FileLinkType.DIRECTORY, 111L,
                        946681200491L / 1000L, 0, 0, (short) 0, 0), idCache).describeLink(true,
                false));
        assertEquals("755\t0\t0\t       111\t2000-01-01 00:00:00\t" + Utils.crc32ToString(200)
                + "\tdir/link_name", new ArchiveEntry("dir", "dir/link_name", new LinkRecord(null,
                null, FileLinkType.REGULAR_FILE, 111L, 946681200491L / 1000L, 0, 0, (short) 0755,
                200), idCache).describeLink(true, true));
        assertEquals("0\t0\t0\t       DIR\t2000-01-01 00:00:00\t        \tdir/link_name",
                new ArchiveEntry("dir", "dir/link_name", new LinkRecord("link_name2", null,
                        FileLinkType.DIRECTORY, 111L, 946681200491L / 1000L, 0, 0, (short) 0, 0),
                        idCache).describeLink(true, true));
        assertEquals("       111\t2000-01-01 00:00:00\t00000000\tdir/link_name", new ArchiveEntry(
                "dir", "dir/link_name", new LinkRecord("link_name", null,
                        FileLinkType.REGULAR_FILE, 111L, 946681200491L / 1000L, -1, 0,
                        (short) 0755, 0), idCache).describeLink(true, false));
        assertEquals("       111\t00000000\tdir/link_name", new ArchiveEntry("dir",
                "dir/link_name", new LinkRecord("link_name2", null, FileLinkType.REGULAR_FILE,
                        111L, -1L, -1, 0, (short) 0755, 0), idCache).describeLink(true, false));
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
        assertEquals("-17", idCache.getUser(-17, true));
        assertEquals("root", idCache.getUser(0, false));
        assertEquals(uname, idCache.getUser(uid, false));
        Integer invalidUid = getInvalidUid();
        if (invalidUid != null)
        {
            assertEquals(Integer.toString(invalidUid), idCache.getUser(invalidUid, false));
        }
    }

    private Integer getInvalidUid()
    {
        int invalidUid;
        for (invalidUid = 60000; invalidUid < 65535 && Unix.tryGetUserByUid(invalidUid) != null; ++invalidUid)
        {
        }
        return Unix.tryGetUserByUid(invalidUid) == null ? invalidUid : null;
    }

    private void writeToArchive(final IHDF5Archiver a, final String name, final String content)
    {
        final byte[] bytes = content.getBytes();
        a.archiveFile(NewArchiveEntry.file("/test", name).lastModified(1000000L).uid(100).gid(100),
                new ByteArrayInputStream(bytes), null);
    }

    @Test
    public void testWriteByteArrayToArchive()
    {
        final File file = new File(workingDirectory, "writeByteArrayToArchive.h5ar");
        file.delete();
        file.deleteOnExit();
        final IHDF5Archiver a = HDF5ArchiverFactory.open(file);
        writeToArchive(a, "hello.txt", "Hello World\n");
        writeToArchive(a, "hello2.txt", "Yet another Hello World\n");
        a.close();
        final IHDF5ArchiveReader aro = HDF5ArchiverFactory.openForReading(file);
        final String content1 = new String(aro.extractFileAsByteArray("/test/hello.txt"));
        assertEquals("Hello World\n", content1);
        final String content2 = new String(aro.extractFileAsByteArray("/test/hello2.txt"));
        assertEquals("Yet another Hello World\n", content2);
        final List<ArchiveEntry> list =
                aro.list("/", ListParameters.build().nonRecursive().noReadLinkTarget().get());
        assertEquals(1, list.size());
        assertEquals("755\t100\t100\t       DIR\t1970-01-12 14:46:40\t        \t/test", list.get(0)
                .describeLink(true, true));
        final List<ArchiveEntry> list2 =
                aro.list("/test", ListParameters.build().testArchive().suppressDirectoryEntries()
                        .get());
        assertEquals(2, list2.size());
        assertEquals(
                "755\t100\t100\t        12\t1970-01-12 14:46:40\tb095e5e3\t/test/hello.txt\tOK",
                list2.get(0).describeLink(true, true));
        assertEquals(
                "755\t100\t100\t        24\t1970-01-12 14:46:40\tee5f3107\t/test/hello2.txt\tOK",
                list2.get(1).describeLink(true, true));
        aro.close();
    }

    @Test
    public void testWriteFileAsOutputStream() throws Exception
    {
        final File file = new File(workingDirectory, "writeFileAsOutputStream.h5ar");
        file.delete();
        file.deleteOnExit();
        final IHDF5Archiver a = HDF5ArchiverFactory.open(file);
        final PrintStream ps =
                new PrintStream(a.archiveFileAsOutputStream(NewArchiveEntry.file("test1")
                        .chunkSize(128)));
        ps.printf("Some %s stuff: %d\n", "more", 17);
        // Note: we don't close the PrintStream or the underlying OutputStream explicitly.
        // The flushables take care of things getting written correctly anyway.
        a.close();
        final IHDF5ArchiveReader ar = HDF5ArchiverFactory.openForReading(file);
        final List<ArchiveEntry> entries = ar.list("", ListParameters.build().testArchive().get());
        assertEquals(1, entries.size());
        assertEquals("test1", entries.get(0).getName());
        assertTrue(entries.get(0).checkOK());
        final BufferedReader r =
                new BufferedReader(new InputStreamReader(ar.extractFileAsInputStream("test1")));
        assertEquals("Some more stuff: 17", r.readLine());
        assertNull(r.readLine());
        ar.close();
    }
}
