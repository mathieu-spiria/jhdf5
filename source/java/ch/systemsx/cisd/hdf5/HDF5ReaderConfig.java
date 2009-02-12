/*
 * Copyright 2009 ETH Zuerich, CISD
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

import static ch.systemsx.cisd.hdf5.HDF5Utils.BOOLEAN_DATA_TYPE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.DATATYPE_GROUP;
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_DATA_TYPE;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

import ch.systemsx.cisd.common.process.CleanUpCallable;
import ch.systemsx.cisd.common.process.CleanUpRegistry;
import ch.systemsx.cisd.common.utilities.OSUtilities;

/**
 * If you want the reader to perform numeric conversions, call {@link #performNumericConversions()}
 * before calling {@link #reader()}.
 * 
 * @author Bernd Rinn
 */
public class HDF5ReaderConfig
{

    /** State that this reader / writer is currently in. */
    protected enum State
    {
        CONFIG, OPEN, CLOSED
    }

    protected final File hdf5File;

    protected final CleanUpCallable runner;

    protected final CleanUpRegistry fileRegistry;

    /** Map from named data types to ids. */
    final Map<String, Integer> namedDataTypeMap;

    protected HDF5 h5;

    protected int fileId;

    protected int booleanDataTypeId;

    protected HDF5EnumerationType typeVariantDataType;

    boolean performNumericConversions;

    protected State state;

    public HDF5ReaderConfig(File hdf5File)
    {
        assert hdf5File != null;

        this.runner = new CleanUpCallable();
        this.fileRegistry = new CleanUpRegistry();
        this.namedDataTypeMap = new HashMap<String, Integer>();
        this.hdf5File = hdf5File.getAbsoluteFile();
    }

    protected void checkOpen() throws HDF5JavaException
    {
        if (state != State.OPEN)
        {
            final String msg =
                    "HDF5 file '" + hdf5File.getPath() + "' is "
                            + (state == State.CLOSED ? "closed." : "not opened yet.");
            throw new HDF5JavaException(msg);
        }
    }

    void open(HDF5Reader reader)
    {
        final String path = hdf5File.getAbsolutePath();
        if (hdf5File.exists() == false)
        {
            throw new IllegalArgumentException("The file " + path + " does not exit.");
        }
        h5 = new HDF5(fileRegistry, performNumericConversions);
        fileId = h5.openFileReadOnly(path, fileRegistry);
        state = State.OPEN;
        readNamedDataTypes(reader);
        booleanDataTypeId = openOrCreateBooleanDataType(reader);
        typeVariantDataType = openOrCreateTypeVariantDataType(reader);
    }

    /**
     * Closes this object and the file referenced by this object. This object must not be used after
     * being closed.
     */
    void close()
    {
        fileRegistry.cleanUp(false);
        state = State.CLOSED;
    }

    protected int openOrCreateBooleanDataType(HDF5Reader reader)
    {
        int dataTypeId = getDataTypeId(BOOLEAN_DATA_TYPE, reader);
        if (dataTypeId < 0)
        {
            dataTypeId = createBooleanDataType();
            commitDataType(BOOLEAN_DATA_TYPE, dataTypeId);
        }
        return dataTypeId;
    }

    protected int getDataTypeId(final String dataTypePath, final HDF5Reader reader)
    {
        final Integer dataTypeIdOrNull = namedDataTypeMap.get(dataTypePath);
        if (dataTypeIdOrNull == null)
        {
            // Just in case of data types added to other groups than HDF5Utils.DATATYPE_GROUP
            if (reader.exists(dataTypePath))
            {
                final int dataTypeId = h5.openDataType(fileId, dataTypePath, fileRegistry);
                namedDataTypeMap.put(dataTypePath, dataTypeId);
                return dataTypeId;
            } else
            {
                return -1;
            }
        } else
        {
            return dataTypeIdOrNull;
        }
    }

    protected int createBooleanDataType()
    {
        return h5.createDataTypeEnum(new String[]
            { "FALSE", "TRUE" }, fileRegistry);
    }

    protected HDF5EnumerationType openOrCreateTypeVariantDataType(HDF5Reader reader)
    {
        int dataTypeId = getDataTypeId(TYPE_VARIANT_DATA_TYPE, reader);
        if (dataTypeId < 0)
        {
            return createTypeVariantDataType();
        }
        final int nativeDataTypeId = h5.getNativeDataType(dataTypeId, fileRegistry);
        final String[] typeVariantNames = h5.getNamesForEnumOrCompoundMembers(dataTypeId);
        return new HDF5EnumerationType(fileId, dataTypeId, nativeDataTypeId,
                TYPE_VARIANT_DATA_TYPE, typeVariantNames);
    }

    protected HDF5EnumerationType createTypeVariantDataType()
    {
        final HDF5DataTypeVariant[] typeVariants = HDF5DataTypeVariant.values();
        final String[] typeVariantNames = new String[typeVariants.length];
        for (int i = 0; i < typeVariants.length; ++i)
        {
            typeVariantNames[i] = typeVariants[i].name();
        }
        final int dataTypeId = h5.createDataTypeEnum(typeVariantNames, fileRegistry);
        final int nativeDataTypeId = h5.getNativeDataType(dataTypeId, fileRegistry);
        return new HDF5EnumerationType(fileId, dataTypeId, nativeDataTypeId,
                TYPE_VARIANT_DATA_TYPE, typeVariantNames);
    }

    protected void readNamedDataTypes(HDF5Reader reader)
    {
        if (reader.exists(DATATYPE_GROUP) == false)
        {
            return;
        }
        for (String dataTypePath : reader.getGroupMemberPaths(DATATYPE_GROUP))
        {
            final int dataTypeId = h5.openDataType(fileId, dataTypePath, fileRegistry);
            namedDataTypeMap.put(dataTypePath, dataTypeId);
        }
    }

    protected void commitDataType(final String dataTypePath, final int dataTypeId)
    {
        // Overwrite method in writer.
    }

    /**
     * Returns <code>true</code>, if this platform supports numeric conversions.
     */
    public boolean platformSupportsNumericConversions()
    {
        // On HDF5 1.8.2, numeric conversions on sparcv9 can get us SEGFAULTS for converting between
        // integers and floats.
        if (OSUtilities.getCPUArchitecture().startsWith("sparc"))
        {
            return false;
        }
        return true;
    }

    /**
     * Will try to perform numeric conversions where appropriate if supported by the platform.
     * <p>
     * <strong>Numeric conversions can be platform dependent and are not available on all platforms.
     * Be advised not to rely on numeric conversions if you can help it!</strong>
     */
    public HDF5ReaderConfig performNumericConversions()
    {
        if (platformSupportsNumericConversions() == false)
        {
            return this;
        }
        this.performNumericConversions = true;
        return this;
    }

    /**
     * Returns an {@link HDF5Reader} based on this configuration.
     */
    public HDF5Reader reader()
    {
        return new HDF5Reader(this, true);
    }

}
