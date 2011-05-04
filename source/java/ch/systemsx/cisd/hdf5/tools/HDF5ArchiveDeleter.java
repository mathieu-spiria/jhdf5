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
import java.util.List;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import ch.systemsx.cisd.hdf5.HDF5FactoryProvider;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.SyncMode;

/**
 * A class to delete paths from an <code>h5ar</code> archives.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiveDeleter
{

    private final IHDF5Writer hdf5Writer;

    private final boolean continueOnError;

    public HDF5ArchiveDeleter(File archiveFile, boolean noSync, FileFormat fileFormat,
            boolean continueOnError)
    {
        this(HDF5ArchiveUpdater.createHDF5Writer(archiveFile, fileFormat, noSync), continueOnError);
    }

    static IHDF5Writer createHDF5Writer(File archiveFile, FileFormat fileFormat, boolean noSync)
    {
        final IHDF5WriterConfigurator config = HDF5FactoryProvider.get().configure(archiveFile);
        config.fileFormat(fileFormat);
        config.useUTF8CharacterEncoding();
        if (noSync == false)
        {
            config.syncMode(SyncMode.SYNC);
        }
        return config.writer();
    }

    public HDF5ArchiveDeleter(IHDF5Writer hdf5Writer, boolean continueOnError)
    {
        this.hdf5Writer = hdf5Writer;
        this.continueOnError = continueOnError;
    }

    @SuppressWarnings("null")
    public HDF5ArchiveDeleter delete(List<String> hdf5ObjectPaths, boolean verbose)
    {
        DirectoryIndex indexOrNull = null;
        String lastGroupOrNull = null;
        for (String path : hdf5ObjectPaths)
        {
            String normalizedPath = path;
            if (normalizedPath.endsWith("/"))
            {
                normalizedPath = normalizedPath.substring(0, path.length() - 1);
            }
            int groupDelimIndex = normalizedPath.lastIndexOf('/');
            final String group =
                    (groupDelimIndex < 2) ? "/" : normalizedPath.substring(0, groupDelimIndex);
            if (group.equals(lastGroupOrNull) == false)
            {
                if (indexOrNull != null)
                {
                    indexOrNull.writeIndexToArchive();
                }
                indexOrNull = new DirectoryIndex(hdf5Writer, group, continueOnError, false);
            }
            try
            {
                hdf5Writer.delete(normalizedPath);
                final String name = normalizedPath.substring(groupDelimIndex + 1);
                indexOrNull.remove(name);
                HDF5ArchiveOutputHelper.writeToConsole(normalizedPath, verbose);
            } catch (HDF5Exception ex)
            {
                HDF5ArchiveOutputHelper.dealWithError(new DeleteFromArchiveException(path, ex),
                        continueOnError);
            }
        }
        if (indexOrNull != null)
        {
            indexOrNull.writeIndexToArchive();
        }
        return this;
    }

    public void close()
    {
        hdf5Writer.close();
    }

}
