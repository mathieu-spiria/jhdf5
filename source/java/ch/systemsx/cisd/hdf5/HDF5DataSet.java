/*
 * Copyright 2015 ETH Zuerich, SIS
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

package ch.systemsx.cisd.hdf5;

import static hdf.hdf5lib.H5.H5Dclose;
import static hdf.hdf5lib.H5.H5Sclose;

/**
 * An object to represent an HDF5 data set.
 * <p>
 * Open using the {@link IHDF5ObjectReadOnlyInfoProviderHandler#openDataSet(String)} and use it in readers and writers 
 * instead of the data set path. As it caches HDF5 objects, it will speed up the access for repeated access to the 
 * same data set.
 * <p>
 * A typical pattern for using this class is:
 * <pre>
 *    try (final HDF5DataSet ds = reader.object().openDataSet("/path/to/dataset"))
 *    {
 *        for (long bx = 0; bx < 8; ++bx)
 *        {
 *            final float[] dataRead =
 *                    reader.float32().readArrayBlock(ds, length, bx);
 *            ... work with dataRead ...
 *        }
 *    }
 * </pre>
 * Assigning the <code>HDF5DataSet</code> object in a <code>try()</code> block is a recommened practice to ensure that 
 * the underlying HDF5 object is properly closed at the end. 
 * 
 * @author Bernd Rinn
 */
public class HDF5DataSet implements AutoCloseable
{
    private final String datasetPath;

    private final HDF5StorageLayout layout;

    private final long dataspaceId;

    private final long[] maxDimensions;

    private long[] dimensions;

    private long datasetId;

    HDF5DataSet(String datasetPath, long datasetId, long dataspaceId, long[] dimensions,
            long[] maxDimensions, HDF5StorageLayout layout)
    {
        this.datasetPath = datasetPath;
        this.datasetId = datasetId;
        this.dataspaceId = dataspaceId;
        this.maxDimensions = maxDimensions;
        this.dimensions = dimensions;
        this.layout = layout;
    }

    /**
     * Returns the path of this data set.
     */
    public String getDatasetPath()
    {
        return datasetPath;
    }

    long getDatasetId()
    {
        return datasetId;
    }

    long getDataspaceId()
    {
        return dataspaceId;
    }

    long[] getDimensions()
    {
        return dimensions;
    }

    void setDimensions(long[] dimensions)
    {
        this.dimensions = dimensions;
    }

    long[] getMaxDimensions()
    {
        return maxDimensions;
    }

    HDF5StorageLayout getLayout()
    {
        return layout;
    }

    @Override
    public void close()
    {
        if (datasetId > 0)
        {
            H5Sclose(dataspaceId);
            H5Dclose(datasetId);
            datasetId = -1;
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((datasetPath == null) ? 0 : datasetPath.hashCode());
        result = prime * result + (int) datasetId;
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        HDF5DataSet other = (HDF5DataSet) obj;
        if (datasetPath == null)
        {
            if (other.datasetPath != null)
            {
                return false;
            }
        } else if (!datasetPath.equals(other.datasetPath))
        {
            return false;
        }
        if (datasetId != other.datasetId)
        {
            return false;
        }
        return true;
    }

}
