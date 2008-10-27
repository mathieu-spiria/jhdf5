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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import ch.systemsx.cisd.hdf5.HDF5Reader;
import ch.systemsx.cisd.hdf5.HDF5Writer;

/**
 * An archiver based on HDF5 as archive format for directory with fast random access to particular
 * files.
 * 
 * @author Bernd Rinn
 */
public class HDF5Archiver
{

    private final HDF5Writer hdf5WriterOrNull;

    private final HDF5Reader hdf5Reader;

    private final ArchivingStrategy strategy;

    private final boolean continueOnError;

    public HDF5Archiver(File archiveFile, boolean readOnly, boolean useLatestFileFormat,
            boolean continueOnError)
    {
        if (readOnly)
        {
            this.hdf5WriterOrNull = null;
            this.hdf5Reader = new HDF5Reader(archiveFile);
        } else
        {
            this.hdf5WriterOrNull = new HDF5Writer(archiveFile);
            if (useLatestFileFormat)
            {
                hdf5WriterOrNull.useLatestFileFormat();
            }
            this.hdf5Reader = hdf5WriterOrNull;
        }
        hdf5Reader.open();
        this.continueOnError = continueOnError;
        this.strategy = new ArchivingStrategy();
    }

    public ArchivingStrategy getStrategy()
    {
        return strategy;
    }

    public HDF5Archiver archiveAll(File path, boolean addEmptyDirectories, boolean verbose)
            throws IllegalStateException
    {
        final File absolutePath = path.getAbsoluteFile();
        return archive(absolutePath.getParentFile(), absolutePath, addEmptyDirectories, verbose);
    }

    public HDF5Archiver archive(File root, File path, boolean addEmptyDirectories, boolean verbose)
            throws IllegalStateException
    {
        if (hdf5WriterOrNull == null)
        {
            throw new IllegalStateException("Cannot archive in read-only mode.");
        }
        HDF5ArchiveTools.archive(hdf5WriterOrNull, strategy, root.getAbsoluteFile(), path
                .getAbsoluteFile(), addEmptyDirectories, continueOnError, verbose);
        return this;
    }

    public HDF5Archiver extract(File root, String path, boolean createEmptyDirectories,
            boolean verbose) throws IllegalStateException
    {
        HDF5ArchiveTools.extract(hdf5Reader, strategy, root, path, createEmptyDirectories,
                continueOnError, verbose);
        return this;
    }

    public HDF5Archiver delete(String hdf5ObjectPath, boolean verbose)
    {
        if (hdf5WriterOrNull == null)
        {
            // delete() must not be called if we only have a reader.
            throw new IllegalStateException("Cannot delete in read-only mode.");
        }
        try
        {
            hdf5WriterOrNull.delete(hdf5ObjectPath);
            HDF5ArchiveTools.list(hdf5ObjectPath, verbose);
        } catch (HDF5Exception ex)
        {
            HDF5ArchiveTools.dealWithError(new DeleteFromArchiveException(hdf5ObjectPath, ex),
                    continueOnError);
        }
        return this;
    }

    public List<String> list(boolean addDirectories, boolean verbose)
    {
        final List<String> result = new ArrayList<String>();
        HDF5ArchiveTools.addEntries(hdf5Reader, result, "/", verbose, addDirectories);
        return result;
    }

    public void close()
    {
        hdf5Reader.close();
    }

}
