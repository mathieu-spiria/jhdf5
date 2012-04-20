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
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
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
                new ByteArrayInputStream(bytes));
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
        assertTrue(entries.get(0).isOK());
        final BufferedReader r =
                new BufferedReader(new InputStreamReader(ar.extractFileAsInputStream("test1")));
        assertEquals("Some more stuff: 17", r.readLine());
        assertNull(r.readLine());
        ar.close();
    }

    private File createTestDirectory() throws IOException
    {
        return createTestDirectory(null);
    }

    private File createTestDirectory(String prefixOrNull) throws IOException
    {
        final File dir =
                (prefixOrNull != null) ? new File(new File(workingDirectory, prefixOrNull), "test")
                        : new File(workingDirectory, "test");
        dir.delete();
        dir.deleteOnExit();
        dir.mkdirs();
        final File f1 = new File(dir, "file_test1.txt");
        f1.delete();
        f1.deleteOnExit();
        FileUtils.writeLines(f1, Arrays.asList("Line 1", "Line 2", "Line 3"));
        final File dir2 = new File(dir, "dir_somedir");
        dir2.delete();
        dir2.mkdir();
        dir2.deleteOnExit();
        final File f2 = new File(dir2, "file_test2.txt");
        f2.delete();
        f2.deleteOnExit();
        FileUtils.writeLines(f2, Arrays.asList("A", "B", "C"));
        final File dir3 = new File(dir, "dir_someotherdir");
        dir3.delete();
        dir3.mkdir();
        dir3.deleteOnExit();
        if (Unix.isOperational())
        {
            final File l1 = new File(dir2, "link_todir3");
            l1.delete();
            l1.deleteOnExit();
            Unix.createSymbolicLink("../" + dir3.getName(), l1.getAbsolutePath());
        }
        return dir;
    }

    @Test
    public void testCreateVerifyRoundtripOK() throws IOException
    {
        final File dir = createTestDirectory();
        final File h5arfile = new File(workingDirectory, "testRoundtrip.h5ar");
        h5arfile.delete();
        h5arfile.deleteOnExit();
        HDF5ArchiverFactory.open(h5arfile).archiveFromFilesystem(dir).close();
        final IHDF5ArchiveReader ar = HDF5ArchiverFactory.openForReading(h5arfile);
        assertTrue(ar.test().isEmpty());
        assertTrue(ar.verifyAgainstFilesystem(dir.getAbsolutePath()).isEmpty());
        ar.close();
    }

    @Test
    public void testCreateVerifyContentArtificialRootRoundtripOK() throws IOException
    {
        final File dir = createTestDirectory();
        final File h5arfile = new File(workingDirectory, "testRoundtripContentArtificialRoot.h5ar");
        h5arfile.delete();
        h5arfile.deleteOnExit();
        HDF5ArchiverFactory.open(h5arfile).archiveFromFilesystemBelowDirectory("ttt", dir).close();
        final IHDF5ArchiveReader ar = HDF5ArchiverFactory.openForReading(h5arfile);
        assertTrue(ar.test().isEmpty());
        assertTrue(ar.verifyAgainstFilesystem("", dir.getAbsolutePath(), "ttt").isEmpty());
        ar.close();
    }

    @Test
    public void testRoundtripArtificalRootOK() throws IOException
    {
        final File dir = createTestDirectory();
        final File h5arfile = new File(workingDirectory, "testRoundtripArtificalRootOK.h5ar");
        h5arfile.delete();
        h5arfile.deleteOnExit();
        HDF5ArchiverFactory.open(h5arfile).archiveFromFilesystem("ttt", dir).close();
        final IHDF5ArchiveReader ar = HDF5ArchiverFactory.openForReading(h5arfile);
        final List<ArchiveEntry> list = ar.list("/");
        assertEquals(7, list.size());
        assertEquals("/ttt", list.get(0).getPath());
        assertEquals("/ttt/test", list.get(1).getPath());
        assertEquals("/ttt/test/dir_somedir", list.get(2).getPath());
        assertEquals("/ttt/test/dir_somedir/file_test2.txt", list.get(3).getPath());
        assertEquals("/ttt/test/dir_somedir/link_todir3", list.get(4).getPath());
        assertEquals("/ttt/test/dir_someotherdir", list.get(5).getPath());
        assertEquals("/ttt/test/file_test1.txt", list.get(6).getPath());
        assertEquals("Line 1\nLine 2\nLine 3\n",
                new String(ar.extractFileAsByteArray("/ttt/test/file_test1.txt")));
        assertEquals("A\nB\nC\n",
                new String(ar.extractFileAsByteArray("/ttt/test/dir_somedir/file_test2.txt")));
        assertTrue(ar.test().isEmpty());
        List<ArchiveEntry> verifyErrors =
                ar.verifyAgainstFilesystem("/", dir.getParentFile().getAbsolutePath(), "/ttt");
        assertTrue(verifyErrors.toString(), verifyErrors.isEmpty());

        final List<ArchiveEntry> list2 = ar.list("/ttt/test/dir_somedir");
        assertEquals(2, list2.size());
        assertEquals("file_test2.txt", list2.get(0).getName());
        assertEquals("link_todir3", list2.get(1).getName());

        final List<ArchiveEntry> list3 =
                ar.list("/ttt/test/dir_somedir", ListParameters.build()
                        .includeTopLevelDirectoryEntry().get());
        assertEquals(3, list3.size());
        assertEquals("dir_somedir", list3.get(0).getName());
        assertEquals("file_test2.txt", list3.get(1).getName());
        assertEquals("link_todir3", list3.get(2).getName());

        ar.close();
    }

    @Test
    public void testRoundtripArtificalRootWhichExistsOnFSOK() throws IOException
    {
        final File dir = createTestDirectory("ttt");
        // Set some special last modified time and access mode that we can recognize
        dir.getParentFile().setLastModified(111000L);
        Unix.setAccessMode(dir.getParent(), (short) 0777);
        final File h5arfile =
                new File(workingDirectory, "testRoundtripArtificalRootWhichExistsOnFSOK.h5ar");
        h5arfile.delete();
        h5arfile.deleteOnExit();
        HDF5ArchiverFactory.open(h5arfile).archiveFromFilesystem("ttt", dir).close();
        final IHDF5ArchiveReader ar = HDF5ArchiverFactory.openForReading(h5arfile);
        final List<ArchiveEntry> list = ar.list("/");
        assertEquals(7, list.size());
        assertEquals("/ttt", list.get(0).getPath());
        // Does the archive entry have the last modified time and access mode we have set in the
        // filesystem?
        assertEquals(111, list.get(0).getLastModified());
        assertEquals((short) 0777, list.get(0).getPermissions());
        assertEquals("/ttt/test", list.get(1).getPath());
        assertEquals("/ttt/test/dir_somedir", list.get(2).getPath());
        assertEquals("/ttt/test/dir_somedir/file_test2.txt", list.get(3).getPath());
        assertEquals("/ttt/test/dir_somedir/link_todir3", list.get(4).getPath());
        assertEquals("/ttt/test/dir_someotherdir", list.get(5).getPath());
        assertEquals("/ttt/test/file_test1.txt", list.get(6).getPath());
        assertEquals("Line 1\nLine 2\nLine 3\n",
                new String(ar.extractFileAsByteArray("/ttt/test/file_test1.txt")));
        assertEquals("A\nB\nC\n",
                new String(ar.extractFileAsByteArray("/ttt/test/dir_somedir/file_test2.txt")));
        assertTrue(ar.test().isEmpty());
        List<ArchiveEntry> verifyErrors =
                ar.verifyAgainstFilesystem("/", dir.getParentFile().getAbsolutePath(), "/ttt");
        assertTrue(verifyErrors.toString(), verifyErrors.isEmpty());
        ar.close();
    }

    @Test
    public void testGetInfo() throws IOException
    {
        final File dir = createTestDirectory();
        final File h5arfile = new File(workingDirectory, "testGetInfo.h5ar");
        h5arfile.delete();
        h5arfile.deleteOnExit();
        HDF5ArchiverFactory.open(h5arfile).archiveFromFilesystem(dir).close();
        final IHDF5ArchiveReader ar = HDF5ArchiverFactory.openForReading(h5arfile);
        assertTrue(ar.exists("file_test1.txt"));
        assertTrue(ar.isRegularFile("file_test1.txt"));
        assertFalse(ar.isDirectory("file_test1.txt"));
        assertFalse(ar.isSymLink("file_test1.txt"));
        assertTrue(ar.exists("dir_somedir"));
        assertFalse(ar.isRegularFile("dir_somedir"));
        assertFalse(ar.isSymLink("dir_somedir"));
        assertTrue(ar.isDirectory("dir_somedir"));
        assertTrue(ar.exists("dir_somedir/link_todir3"));
        assertFalse(ar.isRegularFile("dir_somedir/link_todir3"));
        assertFalse(ar.isDirectory("dir_somedir/link_todir3"));
        assertTrue(ar.isSymLink("dir_somedir/link_todir3"));
        assertEquals("../dir_someotherdir", ar.tryGetEntry("dir_somedir/link_todir3", true)
                .getLinkTarget());
        ar.close();
    }

    private void checkSorted(List<ArchiveEntry> entries)
    {
        boolean dirs = true;
        for (int i = 1; i < entries.size(); ++i)
        {
            if (dirs && entries.get(i).isDirectory() == false)
            {
                dirs = false;
            } else
            {
                assertTrue(entries.get(i - 1).getName().compareTo(entries.get(i).getName()) < 0);
            }
        }
    }

    @Test
    public void testManyFiles()
    {
        workingDirectory.mkdirs();
        final File h5arfile = new File(workingDirectory, "testManyFiles.h5ar");
        h5arfile.delete();
        h5arfile.deleteOnExit();
        final IHDF5Archiver a = HDF5ArchiverFactory.open(h5arfile);
        for (int i = 999; i >= 0; --i)
        {
            a.archiveFile(Integer.toString(i), new byte[0]);
        }
        a.archiveSymlink("symlink", "500");
        a.archiveDirectory(NewArchiveEntry.directory("/dir"));
        a.archiveFile("dir/hello", "hello world".getBytes());
        final List<ArchiveEntry> entries = a.list("/");
        assertEquals(1003, entries.size());
        final ArchiveEntry symLinkEntry = a.tryGetEntry("symlink", true);
        assertNotNull(symLinkEntry);
        assertTrue(symLinkEntry.isSymLink());
        assertTrue(symLinkEntry.hasLinkTarget());
        assertEquals("500", symLinkEntry.getLinkTarget());
        final ArchiveEntry dirEntry = a.tryGetEntry("dir", true);
        assertNotNull(dirEntry);
        assertTrue(dirEntry.isDirectory());
        assertFalse(dirEntry.isRegularFile());
        assertFalse(dirEntry.isSymLink());

        final List<ArchiveEntry> entriesDir = a.list("/dir");
        assertEquals(1, entriesDir.size());
        assertEquals("hello", entriesDir.get(0).getName());
        a.close();
        final IHDF5ArchiveReader ra = HDF5ArchiverFactory.openForReading(h5arfile);
        final List<ArchiveEntry> entriesRead =
                ra.list("/", ListParameters.build().nonRecursive().get());
        assertEquals(1002, entriesRead.size());
        checkSorted(entriesRead);
        for (int i = 1; i < entriesRead.size() - 1; ++i)
        {
            assertTrue(entriesRead.get(i).isRegularFile());
        }
        assertTrue(entriesRead.get(0).isDirectory());
        assertTrue(entriesRead.get(entriesRead.size() - 1).isSymLink());
        for (int i = 1; i < 1001; ++i)
        {
            assertTrue(ra.isRegularFile(Integer.toString(i - 1)));
            assertFalse(ra.isDirectory(Integer.toString(i - 1)));
            assertFalse(ra.isSymLink(Integer.toString(i - 1)));
        }
        assertTrue(ra.isSymLink("symlink"));
        assertFalse(ra.isDirectory("symlink"));
        assertFalse(ra.isRegularFile("symlink"));
        assertEquals("500", ra.tryGetEntry("symlink", true).getLinkTarget());
        assertTrue(ra.isDirectory("dir"));
        assertFalse(ra.isSymLink("dir"));
        assertFalse(ra.isRegularFile("dir"));
        ra.close();
    }

    @Test
    public void testResolveLinks()
    {
        workingDirectory.mkdirs();
        final File h5arfile = new File(workingDirectory, "testResolveLinks.h5ar");
        h5arfile.delete();
        h5arfile.deleteOnExit();
        final IHDF5Archiver a = HDF5ArchiverFactory.open(h5arfile);
        a.archiveFile(NewArchiveEntry.file("aFile"), "Some file content".getBytes());
        a.archiveDirectory(NewArchiveEntry.directory("aDir"));
        a.archiveSymlink(NewArchiveEntry.symlink("aLinkToAFile", "aFile"));
        a.archiveSymlink(NewArchiveEntry.symlink("aLinkToADir", "aDir"));
        a.archiveSymlink(NewArchiveEntry.symlink("aNonsenseLink", "../outOfFS"));
        a.archiveSymlink(NewArchiveEntry.symlink("aLinkToANonexistingFile", "nonexistingFile"));
        a.archiveSymlink(NewArchiveEntry.symlink("aDir/aLinkToALinkToAFile", "../aLinkToAFile"));
        a.archiveSymlink(NewArchiveEntry.symlink("aDir/aLinkToALinkToADir", "/aLinkToADir"));
        
        // A loop
        a.archiveDirectory(NewArchiveEntry.directory("z"));
        a.archiveSymlink(NewArchiveEntry.symlink("z/y", ".."));
        a.close();
        final IHDF5ArchiveReader ra = HDF5ArchiverFactory.openForReading(h5arfile);

        // A file is resolved to itself
        final ArchiveEntry aFileLink = ra.tryGetEntry("aFile", false);
        assertEquals(aFileLink, ra.tryResolveLink(aFileLink));

        // A directory is resolved to itself
        final ArchiveEntry aDirLink = ra.tryGetEntry("aDir", false);
        assertEquals(aDirLink, ra.tryResolveLink(aDirLink));

        // A symlink to a file is correctly resolved...
        final ArchiveEntry aSymLinkToAFile = ra.tryGetEntry("aLinkToAFile", true);
        final ArchiveEntry aResolvedLinkToAFile = ra.tryResolveLink(aSymLinkToAFile);
        assertNotNull(aResolvedLinkToAFile);
        assertEquals(aFileLink.getPath(), aResolvedLinkToAFile.getPath());
        // .. even when the link target was not read
        final ArchiveEntry aSymLinkToAFileWithoutTarget = ra.tryGetEntry("aLinkToAFile", false);
        final ArchiveEntry aResolvedLinkToAFileWithoutTarget =
                ra.tryResolveLink(aSymLinkToAFileWithoutTarget);
        assertNotNull(aResolvedLinkToAFileWithoutTarget);
        assertEquals(aFileLink.getPath(), aResolvedLinkToAFileWithoutTarget.getPath());

        // A symlink to a dir is correctly resolved as well
        final ArchiveEntry aSymLinkToADir = ra.tryGetEntry("aLinkToADir", true);
        final ArchiveEntry aResolvedLinkToADir = ra.tryResolveLink(aSymLinkToADir);
        assertNotNull(aResolvedLinkToADir);
        assertEquals(aDirLink.getPath(), aResolvedLinkToADir.getPath());

        // A nonsense link ('/../outOfFS') is resolved to null
        assertNull(ra.tryResolveLink(ra.tryGetEntry("aNonsenseLink", true)));

        // A link to a non-existing file is resolved to null
        assertNull(ra.tryResolveLink(ra.tryGetEntry("aLinkToANonexistingFile", true)));

        // A link to a link to a file
        final ArchiveEntry aSymLinkToALinkToAFile =
                ra.tryGetEntry("/aDir/aLinkToALinkToAFile", false);
        final ArchiveEntry aResolvedSymLinkToALinkToAFile =
                ra.tryResolveLink(aSymLinkToALinkToAFile);
        assertNotNull(aResolvedSymLinkToALinkToAFile);
        assertEquals(aFileLink.getPath(), aResolvedSymLinkToALinkToAFile.getPath());
        final ArchiveEntry aSymLinkToALinkToAFileWithPathInfoKept =
                ra.tryGetResolvedEntry("/aDir/aLinkToALinkToAFile", true);
        assertEquals("/aDir", aSymLinkToALinkToAFileWithPathInfoKept.getParentPath());
        assertEquals("aLinkToALinkToAFile", aSymLinkToALinkToAFileWithPathInfoKept.getName());
        assertTrue(aSymLinkToALinkToAFileWithPathInfoKept.isRegularFile());
        assertEquals(ra.tryGetEntry("aFile", false).getSize(),
                aSymLinkToALinkToAFileWithPathInfoKept.getSize());

        // A link to a link to a dir
        final ArchiveEntry aSymLinkToALinkToADir =
                ra.tryGetEntry("/aDir/aLinkToALinkToADir", false);
        final ArchiveEntry aResolvedSymLinkToALinkToADir = ra.tryResolveLink(aSymLinkToALinkToADir);
        assertNotNull(aResolvedSymLinkToALinkToADir);
        assertEquals(aDirLink.getPath(), aResolvedSymLinkToALinkToADir.getPath());

        final List<ArchiveEntry> entries =
                ra.list("/", ListParameters.build().resolveSymbolicLinks().get());
        for (ArchiveEntry entry : entries)
        {
            System.err.println(entry);
        }
        assertEquals(8, entries.size());
        assertEquals("/aDir", entries.get(0).getPath());
        assertTrue(entries.get(0).isDirectory());
        assertEquals("/aDir/aLinkToALinkToADir", entries.get(1).getPath());
        assertTrue(entries.get(1).isDirectory());
        assertEquals("/aDir/aLinkToALinkToAFile", entries.get(2).getPath());
        assertTrue(entries.get(2).isRegularFile());
        assertEquals("/z", entries.get(3).getPath());
        assertTrue(entries.get(3).isDirectory());
        assertEquals("/z/y", entries.get(4).getPath());
        assertTrue(entries.get(4).isDirectory());
        assertEquals("/aFile", entries.get(5).getPath());
        assertTrue(entries.get(5).isRegularFile());
        assertEquals("/aLinkToADir", entries.get(6).getPath());
        assertTrue(entries.get(6).isDirectory());
        assertEquals("/aLinkToAFile", entries.get(7).getPath());
        assertTrue(entries.get(7).isRegularFile());
        assertEquals(entries.get(5).getCrc32(), entries.get(7).getCrc32());

        assertEquals("/", ra.tryGetResolvedEntry("z/y", false).getPath());
        
        ra.close();
    }

    @Test
    public void testResolveLinksWithLoops()
    {
        workingDirectory.mkdirs();
        final File h5arfile = new File(workingDirectory, "testResolveLinksWithLoops.h5ar");
        h5arfile.delete();
        h5arfile.deleteOnExit();
        final IHDF5Archiver a = HDF5ArchiverFactory.open(h5arfile);
        a.archiveSymlink(NewArchiveEntry.symlink("a", "b"));
        a.archiveSymlink(NewArchiveEntry.symlink("b", "a"));
        
        a.archiveSymlink(NewArchiveEntry.symlink("c", "d"));
        a.archiveSymlink(NewArchiveEntry.symlink("d", "e"));
        a.archiveSymlink(NewArchiveEntry.symlink("e", "c"));
        a.close();
        final IHDF5ArchiveReader ra = HDF5ArchiverFactory.openForReading(h5arfile);
        assertNull(ra.tryGetResolvedEntry("a", false));
        assertNull(ra.tryGetResolvedEntry("d", false));
        ra.close();
    }
}
