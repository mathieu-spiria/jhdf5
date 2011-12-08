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
import java.util.List;

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

    public IHDF5ArchiveReader archiveFromFilesystem(File path, ArchivingStrategy strategy)
            throws IllegalStateException;

    public IHDF5ArchiveReader archiveFromFilesystem(File path, IPathVisitor pathVisitorOrNull)
            throws IllegalStateException;

    public IHDF5ArchiveReader archiveFromFilesystem(File path, ArchivingStrategy strategy,
            IPathVisitor pathVisitorOrNull) throws IllegalStateException;

    public IHDF5ArchiveReader archiveFromFilesystem(File root, File path) throws IllegalStateException;

    public IHDF5ArchiveReader archiveFromFilesystem(File root, File path, ArchivingStrategy strategy)
            throws IllegalStateException;

    public IHDF5ArchiveReader archiveFromFilesystem(File root, File path, ArchivingStrategy strategy,
            IPathVisitor pathVisitorOrNull) throws IllegalStateException;

    public IHDF5ArchiveReader archiveFile(String path, byte[] data) throws IllegalStateException;

    public IHDF5ArchiveReader archiveFile(String path, InputStream input) throws IllegalStateException;

    public IHDF5ArchiveReader archiveFile(NewFileArchiveEntry entry, InputStream input)
            throws IllegalStateException, IllegalArgumentException;

    public IHDF5ArchiveReader archiveFile(NewFileArchiveEntry entry, byte[] data)
            throws IllegalStateException, IllegalArgumentException;

    public IHDF5ArchiveReader archiveFile(NewFileArchiveEntry entry, InputStream input,
            IPathVisitor pathVisitorOrNull) throws IllegalStateException, IllegalArgumentException;

    public IHDF5ArchiveReader archiveSymlink(NewSymLinkArchiveEntry entry) throws IllegalStateException,
            IllegalArgumentException;

    public IHDF5ArchiveReader archiveSymlink(String path, String linkTarget)
            throws IllegalStateException, IllegalArgumentException;

    public IHDF5ArchiveReader archiveSymlink(NewSymLinkArchiveEntry entry, IPathVisitor pathVisitorOrNull)
            throws IllegalStateException, IllegalArgumentException;

    public IHDF5ArchiveReader archiveDirectory(String path) throws IllegalStateException,
            IllegalArgumentException;

    public IHDF5ArchiveReader archiveDirectory(NewDirectoryArchiveEntry entry)
            throws IllegalStateException, IllegalArgumentException;

    public IHDF5ArchiveReader archiveDirectory(NewDirectoryArchiveEntry entry,
            IPathVisitor pathVisitorOrNull) throws IllegalStateException, IllegalArgumentException;

    public IHDF5ArchiveReader delete(String hdf5ObjectPath);

    public IHDF5ArchiveReader delete(List<String> hdf5ObjectPaths);

    public IHDF5ArchiveReader delete(List<String> hdf5ObjectPaths, IPathVisitor pathVisitorOrNull);

}