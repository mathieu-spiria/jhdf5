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

import ch.rinn.restrictions.Private;
import ch.systemsx.cisd.common.array.MDArray;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * Some utility methods used by {@link HDF5Reader} and {@link HDF5Writer}.
 * 
 * @author Bernd Rinn
 */
final class HDF5Utils
{

    /** The minimal size of a chunk. */
    @Private
    static final int MIN_CHUNK_SIZE = 4;

    /** The minimal size of a data set in order to allow for chunking. */
    private static final long MIN_TOTAL_SIZE_FOR_CHUNKING = 128L;

    /** The dimensions vector for a scalar data type. */
    static final long[] SCALAR_DIMENSIONS = new long[]
        { 1 };

    /** The attribute to signal that this is a variant of the data type. */
    static final String TYPE_VARIANT_ATTRIBUTE = "__TYPE_VARIANT__";

    /** The group to store all named derived data types in. */
    static final String DATATYPE_GROUP = "/__DATA_TYPES__";

    /** The prefix for opqaue data types. */
    static final String OPAQUE_PREFIX = "Opaque_";

    /** The prefix for enum data types. */
    static final String ENUM_PREFIX = "Enum_";

    /** The prefix for time stamp data types. */
    static final String TIMESTAMP_PREFIX = "Timestamp_";

    /** The prefix for compound data types. */
    static final String COMPOUND_PREFIX = "Compound_";

    /** The boolean data type. */
    static final String BOOLEAN_DATA_TYPE = DATATYPE_GROUP + "/" + ENUM_PREFIX + "Boolean";

    /** The timestamp data type for milli-seconds since start of the epoch. */
    static final String TIMESTAMP_DATA_TYPE =
            DATATYPE_GROUP + "/" + TIMESTAMP_PREFIX + "MilliSecondsSinceStartOfTheEpoch";

    /** The data type specifying a type variant. */
    static final String TYPE_VARIANT_DATA_TYPE = DATATYPE_GROUP + "/" + ENUM_PREFIX + "TypeVariant";

    /** The variable-length string data type. */
    static final String VARIABLE_LENGTH_STRING_DATA_TYPE =
            DATATYPE_GROUP + "/String_VariableLength";

    static String getSuperGroup(String path)
    {
        assert path != null;

        final int lastIndexSlash = path.lastIndexOf('/');
        if (lastIndexSlash <= 0)
        {
            return "/";
        } else
        {
            return path.substring(0, lastIndexSlash);
        }
    }

    static boolean isEmpty(long[] dimensions)
    {
        for (long d : dimensions)
        {
            if (d == 0)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the dimensions for a scalar, or <code>null</code>, if this data set is too small for
     * chunking.
     */
    static long[] tryGetChunkSizeForString(int len, boolean tryChunkedDS)
    {
        if (tryChunkedDS)
        {
            return (len < MIN_TOTAL_SIZE_FOR_CHUNKING) ? null : SCALAR_DIMENSIONS;
        } else
        {
            return null;
        }
    }

    /**
     * Returns the dimensions for a String vector, or <code>null</code>, if this data set is too
     * small for chunking.
     */
    static long[] tryGetChunkSizeForStringVector(int dim, int maxLength, boolean tryChunkedDS,
            boolean enforceChunkedDS)
    {
        if (enforceChunkedDS)
        {
            return new long[]
                { dim };
        }
        if (dim * maxLength < MIN_TOTAL_SIZE_FOR_CHUNKING || tryChunkedDS == false)
        {
            return null;
        }
        return new long[]
            { dim };
    }

    /**
     * Returns a chunk size suitable for a data set with <var>dimension</var>, or <code>null</code>,
     * if this data set can't be reasonably chunk-ed.
     */
    static long[] tryGetChunkSize(final long[] dimensions, boolean tryChunkedDS,
            boolean enforceChunkedDS)
    {
        assert dimensions != null;

        if (enforceChunkedDS == false && tryChunkedDS == false)
        {
            return null;
        }
        final long[] chunkSize = new long[dimensions.length];
        long totalSize = 1L;
        for (int i = 0; i < dimensions.length; ++i)
        {
            totalSize *= dimensions[i];
            chunkSize[i] = Math.max(MIN_CHUNK_SIZE, dimensions[i]);
        }
        if (enforceChunkedDS == false && totalSize < MIN_TOTAL_SIZE_FOR_CHUNKING)
        {
            return null;
        }
        return chunkSize;
    }

    /**
     * Returns a path for a data type with <var>name</var> and (optional) <var>appendices</var>.
     */
    static String createDataTypePath(String name, String... appendices)
    {
        final StringBuilder builder = new StringBuilder();
        builder.append(DATATYPE_GROUP);
        builder.append('/');
        builder.append(name);
        for (String app : appendices)
        {
            builder.append(app);
        }
        return builder.toString();
    }

    /**
     * Returns the length of a one-dimension array defined by <var>dimensions</var>.
     * 
     * @throws IllegalArgumentException If <var>dimensions</var> do not define a one-dimensional
     *             array or if <code>dimensions[0]</code> overflows the <code>int</code> type.
     */
    static int getOneDimensionalArraySize(final long[] dimensions)
    {
        assert dimensions != null;

        if (dimensions.length == 0) // Scalar data space needs to be treated differently
        {
            return 1;
        }
        if (dimensions.length != 1)
        {
            throw new HDF5JavaException("Data Set is expected to be of rank 1 (rank="
                    + dimensions.length + ")");
        }
        final int length = (int) dimensions[0];
        if (length != dimensions[0])
        {
            throw new IllegalArgumentException("Length is too large (" + dimensions[0] + ")");
        }
        return length;
    }

    /**
     * Checks that <var>dimensions</var> are of <var>expectedRank</var> and converts them from
     * <code>long[]</code> to <code>int[]</code>.
     */
    static int[] toInt(final int expectedRank, final long[] dimensions)
    {
        assert dimensions != null;

        if (dimensions.length != expectedRank)
        {
            throw new IllegalArgumentException("Data Set is expected to be of rank " + expectedRank
                    + " (rank=" + dimensions.length + ")");
        }
        return MDArray.toInt(dimensions);
    }

    /**
     * Returns <code>true</code>, if <var>name</var> denotes an internal name used by the library
     * for house-keeping.
     */
    static boolean isInternalName(final String name)
    {
        return name.startsWith("__") && name.endsWith("__");
    }

    @SuppressWarnings("unchecked")
    static <T> T[] createArray(final Class<T> componentClass, final int vectorLength)
    {
        final T[] value = (T[]) java.lang.reflect.Array.newInstance(componentClass, vectorLength);
        return value;
    }

}
