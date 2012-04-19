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

import java.util.List;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import ch.systemsx.cisd.hdf5.IHDF5Writer;

/**
 * A class to delete paths from an <code>h5ar</code> archives.
 * 
 * @author Bernd Rinn
 */
class HDF5ArchiveDeleter
{
    private final IHDF5Writer hdf5Writer;

    private final IDirectoryIndexProvider indexProvider;

    public HDF5ArchiveDeleter(IHDF5Writer hdf5Writer, IDirectoryIndexProvider indexProvider)
    {
        this.hdf5Writer = hdf5Writer;
        this.indexProvider = indexProvider;
    }

    public HDF5ArchiveDeleter delete(List<String> hdf5ObjectPaths, IPathVisitor pathVisitorOrNull)
    {
        for (String path : hdf5ObjectPaths)
        {
            final String normalizedPath = Utils.normalizePath(path);
            final String group = Utils.getQuasiParentPath(normalizedPath);
            final IDirectoryIndex index = indexProvider.get(group, false);
            try
            {
                hdf5Writer.delete(normalizedPath);
                final String name = normalizedPath.substring(group.length() + 1);
                index.remove(name);
                if (pathVisitorOrNull != null)
                {
                    pathVisitorOrNull.visit(normalizedPath);
                }
            } catch (HDF5Exception ex)
            {
                indexProvider.getErrorStrategy().dealWithError(
                        new DeleteFromArchiveException(path, ex));
            }
        }
        return this;
    }

}
