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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import ch.systemsx.cisd.base.exceptions.IOExceptionUnchecked;
import ch.systemsx.cisd.base.io.IOutputStream;
import ch.systemsx.cisd.hdf5.h5ar.NewArchiveEntry.NewDirectoryArchiveEntry;
import ch.systemsx.cisd.hdf5.h5ar.NewArchiveEntry.NewFileArchiveEntry;
import ch.systemsx.cisd.hdf5.h5ar.NewArchiveEntry.NewSymLinkArchiveEntry;

/**
 * An interface for the HDF5 archiver.
 *
 * @author Bernd Rinn
 */
public interface IHDF5Archiver extends IHDF5ArchiveReader
{

    public void flush() throws IOException;

    public IHDF5Archiver archiveFromFilesystem(File path) throws IllegalStateException;
    
    public IHDF5Archiver archiveFromFilesystem(File path, ArchivingStrategy strategy);

    public IHDF5Archiver archiveFromFilesystem(File path, IPathVisitor pathVisitorOrNull);

    public IHDF5Archiver archiveFromFilesystem(File path, ArchivingStrategy strategy,
            IPathVisitor pathVisitorOrNull);

    public IHDF5Archiver archiveFromFilesystem(File root, File path);

    public IHDF5Archiver archiveFromFilesystem(File root, File path, ArchivingStrategy strategy);

    public IHDF5Archiver archiveFromFilesystem(File root, File path, ArchivingStrategy strategy,
            IPathVisitor pathVisitorOrNull);

    public IHDF5Archiver archiveFile(String path, byte[] data);

    public IHDF5Archiver archiveFile(String path, InputStream input);

    public IHDF5Archiver archiveFile(NewFileArchiveEntry entry, InputStream input);

    public IOutputStream archiveFileAsIOutputStream(NewFileArchiveEntry entry);

    public OutputStream archiveFileAsOutputStream(NewFileArchiveEntry entry);

    public IHDF5Archiver archiveFile(NewFileArchiveEntry entry, byte[] data);

    public IHDF5Archiver archiveFile(NewFileArchiveEntry entry, InputStream input,
            IPathVisitor pathVisitorOrNull);

    public IHDF5Archiver archiveSymlink(NewSymLinkArchiveEntry entry);

    public IHDF5Archiver archiveSymlink(String path, String linkTarget);

    public IHDF5Archiver archiveSymlink(NewSymLinkArchiveEntry entry, IPathVisitor pathVisitorOrNull);

    public IHDF5Archiver archiveDirectory(String path);

    public IHDF5Archiver archiveDirectory(NewDirectoryArchiveEntry entry);

    public IHDF5Archiver archiveDirectory(NewDirectoryArchiveEntry entry,
            IPathVisitor pathVisitorOrNull);

    public IHDF5Archiver delete(String hdf5ObjectPath);

    public IHDF5Archiver delete(List<String> hdf5ObjectPaths);

    public IHDF5Archiver delete(List<String> hdf5ObjectPaths, IPathVisitor pathVisitorOrNull);

    // Method overridden from IHDF5ArchiveReader
    
    public IHDF5Archiver list(String fileOrDir, IListEntryVisitor visitor);

    public IHDF5Archiver list(String fileOrDir, IListEntryVisitor visitor, ListParameters params);

    public IHDF5Archiver verifyAgainstFilesystem(String fileOrDir, String rootDirectory,
            IListEntryVisitor visitor);

    public IHDF5Archiver verifyAgainstFilesystem(String fileOrDir, String rootDirectory,
            IListEntryVisitor visitor, VerifyParameters params);

    public IHDF5Archiver extractFile(String path, OutputStream out) throws IOExceptionUnchecked;

    public IHDF5Archiver extractToFilesystem(File root, String path) throws IllegalStateException;

    public IHDF5Archiver extractToFilesystem(File root, String path, IListEntryVisitor visitorOrNull)
            throws IllegalStateException;

    public IHDF5Archiver extractToFilesystem(File root, String path, ArchivingStrategy strategy,
            IListEntryVisitor visitorOrNull) throws IllegalStateException;

}