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

import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;

/**
 * A factory for {@link IHDF5Archiver}
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiverFactory
{
    /**
     * Opens an HDF5 archive <var>file</var> for writing and reading.
     * 
     * @param file The archive file to open. If the archive file does not yet exist, it will be
     *            created.
     */
    public static IHDF5Archiver open(File file)
    {
        return new HDF5Archiver(file, false);
    }

    /**
     * Opens an HDF5 archive <var>file</var> for writing and reading.
     * 
     * @param file The archive file to open. If the archive file does not yet exist, it will be
     *            created.
     * @param noSync if <code>true</code>, no <code>sync</code> call will be performed on closing
     *            the file.
     * @param fileFormat The HDF5 file format to use for the archive.
     * @param errorStrategyOrNull The {@link IErrorStrategy} to use on errors when accessing the
     *            archive. May be <code>null</code>, in which case every error just causes an
     *            exception.
     */
    public static IHDF5Archiver open(File file, boolean noSync, FileFormat fileFormat,
            IErrorStrategy errorStrategyOrNull)
    {
        return new HDF5Archiver(file, false, noSync, fileFormat, errorStrategyOrNull);
    }

    /**
     * Opens an HDF5 archive file named <var>filePath</var> for writing and reading.
     * 
     * @param filePath The path of the archive file to open. If the archive file does not yet exist,
     *            it will be created.
     */
    public static IHDF5Archiver open(String filePath)
    {
        return new HDF5Archiver(new File(filePath), false);
    }

    /**
     * Opens an HDF5 archive file named <var>filePath</var> for writing and reading.
     * 
     * @param filePath The path of the archive file to open. If the archive file does not yet exist,
     *            it will be created.
     * @param noSync if <code>true</code>, no <code>sync</code> call will be performed on closing
     *            the file.
     * @param fileFormat The HDF5 file format to use for the archive.
     * @param errorStrategyOrNull The {@link IErrorStrategy} to use on errors when accessing the
     *            archive. May be <code>null</code>, in which case every error just causes an
     *            exception.
     */
    public static IHDF5Archiver open(String filePath, boolean noSync, FileFormat fileFormat,
            IErrorStrategy errorStrategyOrNull)
    {
        return new HDF5Archiver(new File(filePath), false, noSync, fileFormat, errorStrategyOrNull);
    }

    /**
     * Opens an HDF5 archive <var>file</var> for reading.
     * 
     * @param file The archive file to open. It is an error if the archive file does not exist.
     */
    public static IHDF5ArchiveReader openForReading(File file)
    {
        return new HDF5Archiver(file, true);
    }

    /**
     * Opens an HDF5 archive <var>file</var> for reading.
     * 
     * @param file The archive file to open. It is an error if the archive file does not exist.
     * @param errorStrategy The {@link IErrorStrategy} to use on errors when accessing the archive.
     */
    public static IHDF5ArchiveReader openForReading(File file, IErrorStrategy errorStrategy)
    {
        return new HDF5Archiver(file, true, true, FileFormat.ALLOW_1_8, errorStrategy);
    }

    /**
     * Opens an HDF5 archive file named <var>filePath</var> for reading.
     * 
     * @param filePath The path of the archive file to open. It is an error if the archive file does
     *            not exist.
     */
    public static IHDF5ArchiveReader openForReading(String filePath)
    {
        return new HDF5Archiver(new File(filePath), true);
    }

    /**
     * Opens an HDF5 archive file named <var>filePath</var> for reading.
     * 
     * @param filePath The path of the archive file to open. It is an error if the archive file does
     *            not exist.
     * @param errorStrategy The {@link IErrorStrategy} to use on errors when accessing the archive.
     */
    public static IHDF5ArchiveReader openForReading(String filePath, IErrorStrategy errorStrategy)
    {
        return new HDF5Archiver(new File(filePath), true, true, FileFormat.ALLOW_1_8, errorStrategy);
    }

}
