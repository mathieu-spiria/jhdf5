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
import static hdf.hdf5lib.H5.H5Dget_type;
import static hdf.hdf5lib.H5.H5Sclose;
import static hdf.hdf5lib.H5.H5Scopy;
import static hdf.hdf5lib.H5.H5Screate_simple;
import static hdf.hdf5lib.H5.H5Tclose;

import java.util.Arrays;

/**
 * An object to represent an HDF5 data set.
 * <p>
 * <i>Close it after usage is finished as otherwise you will leak resources from the HDF5
 * library.</i>
 * <p>
 * Open the object using the method {@link IHDF5ObjectReadOnlyInfoProviderHandler#openDataSet(String)} and use it
 * in readers and writers instead of the data set path. As it caches HDF5 objects, it will speed up the access
 * for repeated access to the same data set.
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
    private final HDF5BaseReader baseReader;
    
    private final HDF5 h5;
    
    private final String datasetPath;

    private final HDF5StorageLayout layout;

    private long dataspaceId;
    
    private long[] maxDimensions;

    private long[] dimensions;

    private long datasetId;
    
    private long[] memoryBlockDimensions;
    
    private long memorySpaceId;
    
    private long dataTypeId;
    
    private int fullRank;

    HDF5DataSet(HDF5BaseReader baseReader, String datasetPath, long datasetId, long dataspaceId, long[] dimensions,
            long[] maxDimensionsOrNull, HDF5StorageLayout layout, boolean ownDataSpaceId)
    {
        this.baseReader = baseReader;
        this.h5 = baseReader.h5;
        this.datasetPath = datasetPath;
        this.datasetId = datasetId;
        if (ownDataSpaceId)
        {
            this.dataspaceId = dataspaceId;
        } else
        {
            this.dataspaceId = H5Scopy(dataspaceId);
        }
        this.maxDimensions = maxDimensionsOrNull;
        this.dimensions = dimensions;
        this.layout = layout;
        this.memoryBlockDimensions = null;
        this.memorySpaceId = -1;
        this.dataTypeId = -1;
        this.fullRank = -1;
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
    
    long getMemorySpaceId(long[] memoryBlockDimensions)
    {
        if (false == Arrays.equals(this.memoryBlockDimensions, memoryBlockDimensions))
        {
            closeMemorySpaceId();
            this.memoryBlockDimensions = memoryBlockDimensions;
            this.memorySpaceId = H5Screate_simple(memoryBlockDimensions.length, memoryBlockDimensions, null);
        }
        return memorySpaceId;
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
        if (maxDimensions == null)
        {
            this.maxDimensions = h5.getDataSpaceMaxDimensions(dataspaceId);
        }
        return maxDimensions;
    }

    HDF5StorageLayout getLayout()
    {
        return layout;
    }
    
    int getFullRank()
    {
        if (fullRank == -1)
        {
            this.fullRank = baseReader.getRank(datasetPath);
        }
        return fullRank;
    }

    void extend(long[] requiredDimensions)
    {
        final long[] newDimensions = h5.computeNewDimensions(dimensions, requiredDimensions, false);
        if (false == Arrays.equals(dimensions, newDimensions))
        {
            closeDataSpaceId();
            h5.extendDataSet(this, newDimensions, false);
            this.dimensions = newDimensions;
            this.dataspaceId = h5.getDataSpaceForDataSet(datasetId, null);
        }
    }

    long getDataTypeId()
    {
        if (dataTypeId == -1)
        {
            this.dataTypeId = H5Dget_type(datasetId);
        }
        return dataTypeId;
    }

    @Override
    public void close()
    {
        closeDataSetId();
        closeDataSpaceId();
        closeMemorySpaceId();
        closeDataTypeId();
    }

    private void closeDataTypeId()
    {
        if (dataTypeId > -1)
        {
            H5Tclose(dataTypeId);
            dataTypeId = -1;
        }
    }

    private void closeDataSetId()
    {
        if (datasetId > 0)
        {
            H5Dclose(datasetId);
            datasetId = -1;
        }
    }

    private void closeDataSpaceId()
    {
        if (dataspaceId > -1)
        {
            H5Sclose(dataspaceId);
            dataspaceId = -1;
        }
    }
    
    private void closeMemorySpaceId()
    {
        if (memorySpaceId > 0)
        {
            H5Sclose(memorySpaceId);
            memoryBlockDimensions = null;
            memorySpaceId = -1;
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
