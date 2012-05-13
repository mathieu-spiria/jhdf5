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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import ch.systemsx.cisd.base.io.IInputStream;

/**
 * An interface for an HDF5 archive reader.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5ArchiveReader extends IHDF5ArchiveInfoProvider
{

    /**
     * Closes this object and the file referenced by this object. This object must not be used after
     * being closed. Calling this method for a second time is a no-op.
     */
    public void close();

    /**
     * Returns <code>true</code> if this archive reader has been already closed.
     */
    public boolean isClosed();

    //
    // Verification
    //

    /**
     * Verifies the content of the archive against the filesystem.
     * 
     * @param fileOrDir The file or directory entry in the archive to verify. May be empty, in which
     *            case all entries below <var>rootDirectoryInArchive</var> are verified.
     * @param rootDirectoryOnFS The root directory on the file system that should be added to each
     *            entry in the archive when comparing.
     * @param rootDirectoryInArchive The root directory in the archive to start verify from. It will
     *            be stripped from each entry before <var>rootDirectoryOnFS</var> is added.
     * @param visitor The entry visitor to call for each entry. Call {@link ArchiveEntry#isOK()} to
     *            check whether verification was successful.
     * @param params The parameters to determine behavior of the verification process.
     * @return This archive reader.
     */
    public IHDF5ArchiveReader verifyAgainstFilesystem(String fileOrDir, File rootDirectoryOnFS,
            String rootDirectoryInArchive, IArchiveEntryVisitor visitor, VerifyParameters params);

    /**
     * Verifies the content of the complete archive against the filesystem.
     * 
     * @param fileOrDir The file or directory entry in the archive to verify. May be empty, in which
     *            case all entries below <var>rootDirectoryInArchive</var> are verified.
     * @param rootDirectoryOnFS The root directory on the file system that should be added to each
     *            entry in the archive when comparing.
     * @param visitor The entry visitor to call for each entry. Call {@link ArchiveEntry#isOK()} to
     *            check whether verification was successful.
     * @param params The parameters to determine behavior of the verification process.
     * @return This archive reader.
     */
    public IHDF5ArchiveReader verifyAgainstFilesystem(String fileOrDir, File rootDirectoryOnFS,
            IArchiveEntryVisitor visitor, VerifyParameters params);

    /**
     * Verifies the content of the complete archive against the filesystem.
     * 
     * @param fileOrDir The file or directory entry in the archive to verify. May be empty, in which
     *            case all entries below <var>rootDirectoryInArchive</var> are verified.
     * @param rootDirectoryOnFS The root directory on the file system that should be added to each
     *            entry in the archive when comparing.
     * @param visitor The entry visitor to call for each entry. Call {@link ArchiveEntry#isOK()} to
     *            check whether verification was successful.
     * @return This archive reader.
     */
    public IHDF5ArchiveReader verifyAgainstFilesystem(String fileOrDir, File rootDirectoryOnFS,
            IArchiveEntryVisitor visitor);

    /**
     * Verifies the content of the archive against the filesystem.
     * 
     * @param fileOrDir The file or directory entry in the archive to verify. May be empty, in which
     *            case all entries below <var>rootDirectoryInArchive</var> are verified.
     * @param rootDirectoryOnFS The root directory on the file system that should be added to each
     *            entry in the archive when comparing.
     * @param rootDirectoryInArchive The root directory in the archive to start verify from. It will
     *            be stripped from each entry before <var>rootDirectoryOnFS</var> is added.
     * @param params The parameters to determine behavior of the verification process.
     * @return The list of archive entries which failed verification.
     */
    public List<ArchiveEntry> verifyAgainstFilesystem(String fileOrDir, File rootDirectoryOnFS,
            String rootDirectoryInArchive, VerifyParameters params);

    /**
     * Verifies the content of the archive against the filesystem.
     * 
     * @param fileOrDir The file or directory entry in the archive to verify. May be empty, in which
     *            case all entries below <var>rootDirectoryInArchive</var> are verified.
     * @param rootDirectoryOnFS The root directory on the file system that should be added to each
     *            entry in the archive when comparing.
     * @param params The parameters to determine behavior of the verification process.
     * @return The list of archive entries which failed verification.
     */
    public List<ArchiveEntry> verifyAgainstFilesystem(String fileOrDir, File rootDirectoryOnFS,
            VerifyParameters params);

    /**
     * Verifies the content of the archive against the filesystem.
     * 
     * @param fileOrDir The file or directory entry in the archive to verify. May be empty, in which
     *            case all entries below <var>rootDirectoryInArchive</var> are verified.
     * @param rootDirectoryOnFS The root directory on the file system that should be added to each
     *            entry in the archive when comparing.
     * @return The list of archive entries which failed verification.
     */
    public List<ArchiveEntry> verifyAgainstFilesystem(String fileOrDir, File rootDirectoryOnFS);

    /**
     * Verifies the content of the complete archive against the filesystem.
     * 
     * @param rootDirectoryOnFS The root directory on the file system that should be added to each
     *            entry in the archive when comparing.
     * @return The list of archive entries which failed verification.
     */
    public List<ArchiveEntry> verifyAgainstFilesystem(File rootDirectoryOnFS);

    /**
     * Verifies the content of the archive against the filesystem.
     * 
     * @param fileOrDir The file or directory entry in the archive to verify. May be empty, in which
     *            case all entries below <var>rootDirectoryInArchive</var> are verified.
     * @param rootDirectoryOnFS The root directory on the file system that should be added to each
     *            entry in the archive when comparing.
     * @param rootDirectoryInArchive The root directory in the archive to start verify from. It will
     *            be stripped from each entry before <var>rootDirectoryOnFS</var> is added.
     */
    public List<ArchiveEntry> verifyAgainstFilesystem(String fileOrDir, File rootDirectoryOnFS,
            String rootDirectoryInArchive);

    //
    // Extraction
    //

    public IHDF5ArchiveReader extractFile(String path, OutputStream out);

    public byte[] extractFileAsByteArray(String path);

    /**
     * @return If the {@link ch.systemsx.cisd.base.exceptions.IErrorStrategy} of the archive reader
     *         does not re-throw exceptions, the return value will be <code>null</code> on errors.
     */
    public IInputStream extractFileAsIInputStream(String path);

    /**
     * @return If the {@link ch.systemsx.cisd.base.exceptions.IErrorStrategy} of the archive reader
     *         does not re-throw exceptions, the return value will be <code>null</code> on errors.
     */
    public InputStream extractFileAsInputStream(String path);

    public IHDF5ArchiveReader extractToFilesystem(File root, String path);

    public IHDF5ArchiveReader extractToFilesystem(File root, String path,
            IArchiveEntryVisitor visitor);

    public IHDF5ArchiveReader extractToFilesystem(File root, String path,
            ArchivingStrategy strategy, IArchiveEntryVisitor visitor);

}