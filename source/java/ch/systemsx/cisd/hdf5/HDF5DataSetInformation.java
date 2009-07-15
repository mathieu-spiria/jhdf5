/*
 * Copyright 2007 ETH Zuerich, CISD.
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

import ncsa.hdf.hdf5lib.HDF5Constants;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A class that holds relevant information about a data set.
 * 
 * @author Bernd Rinn
 */
public final class HDF5DataSetInformation
{
    /**
     * The storage layout of a data set in the HDF5 file. Not applicable for attributes.
     */
    public enum StorageLayout
    {
        COMPACT(HDF5Constants.H5D_COMPACT), CONTIGUOUS(HDF5Constants.H5D_CONTIGUOUS), CHUNKED(
                HDF5Constants.H5D_CHUNKED), VARIABLE_LENGTH(-1), NOT_APPLICABLE(-1);

        private int id;

        private StorageLayout(int id)
        {
            this.id = id;
        }

        static StorageLayout fromId(int id) throws IllegalArgumentException
        {
            for (StorageLayout layout : values())
            {
                if (layout.id == id)
                {
                    return layout;
                }
            }
            throw new IllegalArgumentException("Illegal layout id " + id);
        }
    }

    private final HDF5DataTypeInformation typeInformation;

    private final HDF5DataTypeVariant typeVariantOrNull;

    private long[] dimensions;

    private long[] maxDimensions;

    private StorageLayout storageLayout = StorageLayout.NOT_APPLICABLE;

    private int[] chunkSizesOrNull;

    HDF5DataSetInformation(HDF5DataTypeInformation typeInformation,
            HDF5DataTypeVariant typeVariantOrNull)
    {
        this.typeInformation = typeInformation;
        this.typeVariantOrNull = typeVariantOrNull;
    }

    /**
     * Returns the data type information for the data set.
     */
    public HDF5DataTypeInformation getTypeInformation()
    {
        return typeInformation;
    }

    /**
     * Returns the data type variant of this data set, or <code>null</code>, if this data set is not
     * tagged with a type variant.
     */
    public HDF5DataTypeVariant tryGetTypeVariant()
    {
        return typeVariantOrNull;
    }

    /**
     * Returns <code>true</code>, if the data set is a time stamp, or <code>false</code> otherwise.
     */
    public boolean isTimeStamp()
    {
        return (typeVariantOrNull != null) ? typeVariantOrNull.isTimeStamp() : false;
    }

    /**
     * Returns <code>true</code>, if the data set is a time duration, or <code>false</code>
     * otherwise.
     */
    public boolean isTimeDuration()
    {
        return (typeVariantOrNull != null) ? typeVariantOrNull.isTimeDuration() : false;
    }

    /**
     * Returns the time unit of the data set, if the data set is a time duration, or
     * <code>null</code> otherwise.
     */
    public HDF5TimeUnit tryGetTimeUnit()
    {
        return (typeVariantOrNull != null) ? typeVariantOrNull.tryGetTimeUnit() : null;
    }

    /**
     * Returns the array dimensions of the data set.
     */
    public long[] getDimensions()
    {
        return dimensions;
    }

    void setDimensions(long[] dimensions)
    {
        this.dimensions = dimensions;
    }

    /**
     * Returns the largest possible array dimensions of the data set.
     */
    public long[] getMaxDimensions()
    {
        return maxDimensions;
    }

    void setMaxDimensions(long[] maxDimensions)
    {
        this.maxDimensions = maxDimensions;
    }

    void setStorageLayout(StorageLayout storageLayout)
    {
        this.storageLayout = storageLayout;
    }

    /**
     * Returns the storage layout of the data set in the HDF5 file.
     */
    public StorageLayout getStorageLayout()
    {
        return storageLayout;
    }

    /**
     * Returns the chunk size in each array dimension of the data set, or <code>null</code>, if the
     * data set is not of {@link StorageLayout#CHUNKED}.
     */
    public int[] tryGetChunkSizes()
    {
        return chunkSizesOrNull;
    }

    void setChunkSizes(int[] chunkSizes)
    {
        this.chunkSizesOrNull = chunkSizes;
    }

    /**
     * Returns the rank (number of axis) of this data set.
     */
    public int getRank()
    {
        return dimensions.length;
    }

    /**
     * Returns <code>true</code>, if the rank of this data set is 0.
     */
    public boolean isScalar()
    {
        return dimensions.length == 0;
    }

    /**
     * Returns the one-dimensional length of the multi-dimensional array defined by
     * <var>dimensions</var>.
     */
    private static long getLength(final long[] dimensions)
    {
        assert dimensions != null;

        if (dimensions.length == 0) // NULL data space needs to be treated differently
        {
            return 0;
        }
        long length = dimensions[0];
        for (int i = 1; i < dimensions.length; ++i)
        {
            length *= dimensions[i];
        }
        return length;
    }

    /**
     * Returns the total number of elements of this data set.
     */
    public long getNumberOfElements()
    {
        return getLength(dimensions);
    }

    /**
     * Returns the total size (in bytes) of this data set.
     */
    public long getSize()
    {
        return getLength(dimensions) * typeInformation.getElementSize();
    }

    //
    // Object
    //

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || obj instanceof HDF5DataSetInformation == false)
        {
            return false;
        }
        final HDF5DataSetInformation that = (HDF5DataSetInformation) obj;
        final EqualsBuilder builder = new EqualsBuilder();
        builder.append(typeInformation, that.typeInformation);
        builder.append(typeVariantOrNull, that.typeVariantOrNull);
        builder.append(dimensions, that.dimensions);
        builder.append(maxDimensions, that.maxDimensions);
        return builder.isEquals();
    }

    @Override
    public int hashCode()
    {
        final HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(typeInformation);
        builder.append(typeVariantOrNull);
        builder.append(dimensions);
        builder.append(maxDimensions);
        return builder.toHashCode();
    }

    @Override
    public String toString()
    {
        if (typeVariantOrNull != null)
        {
            return typeInformation.toString() + "/" + typeVariantOrNull + ":"
                    + ArrayUtils.toString(dimensions);
        } else
        {
            return typeInformation.toString() + ":" + ArrayUtils.toString(dimensions);
        }
    }
}
