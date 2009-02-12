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

import static ch.systemsx.cisd.hdf5.HDF5Utils.DATATYPE_GROUP;
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_DATA_TYPE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.VARIABLE_LENGTH_STRING_DATA_TYPE;

import java.io.File;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * The configuration of this writer config is done by chaining calls to methods {@link #overwrite()}
 * , {@link #dontUseExtendableDataTypes()} and {@link #useLatestFileFormat()} before calling
 * {@link #writer()}.
 * 
 * @author Bernd Rinn
 */
public final class HDF5WriterConfig extends HDF5ReaderConfig
{

    /**
     * A constant that specifies the default deflation level (gzip compression).
     */
    final static int DEFAULT_DEFLATION = 6;

    /**
     * The size threshold for the COMPACT storage layout.
     */
    final static int COMPACT_LAYOUT_THRESHOLD = 256;

    boolean useExtentableDataTypes = true;

    boolean overwrite = false;

    boolean useLatestFileFormat = false;

    int variableLengthStringDataTypeId;

    public HDF5WriterConfig(File hdf5File)
    {
        super(hdf5File);
    }

    @Override
    protected void commitDataType(final String dataTypePath, final int dataTypeId)
    {
        h5.commitDataType(fileId, dataTypePath, dataTypeId);
    }

    @Override
    void open(HDF5Reader reader)
    {
        final String path = hdf5File.getAbsolutePath();
        h5 = new HDF5(fileRegistry, true);
        fileId = openOrCreateFile(path);
        state = State.OPEN;
        readNamedDataTypes(reader);
        booleanDataTypeId = openOrCreateBooleanDataType(reader);
        typeVariantDataType = openOrCreateTypeVariantDataType(reader);
        variableLengthStringDataTypeId = openOrCreateVLStringType(reader);
    }

    private int openOrCreateFile(final String path)
    {
        if (hdf5File.exists() && overwrite == false)
        {
            return h5.openFileReadWrite(path, useLatestFileFormat, fileRegistry);
        } else
        {
            final File directory = hdf5File.getParentFile();
            if (directory.exists() == false)
            {
                throw new HDF5JavaException("Directory '" + directory.getPath()
                        + "' does not exist.");
            }
            return h5.createFile(path, useLatestFileFormat, fileRegistry);
        }
    }

    protected HDF5EnumerationType openOrCreateTypeVariantDataType(final HDF5Writer writer)
    {
        final HDF5EnumerationType dataType;
        int dataTypeId = getDataTypeId(HDF5Utils.TYPE_VARIANT_DATA_TYPE, writer);
        if (dataTypeId < 0
                || h5.getNumberOfMembers(dataTypeId) < HDF5DataTypeVariant.values().length)
        {
            final String typeVariantPath = findFirstUnusedTypeVariantPath(writer);
            dataType = createTypeVariantDataType();
            commitDataType(typeVariantPath, dataType.getStorageTypeId());
            writer.createOrUpdateSoftLink(typeVariantPath.substring(DATATYPE_GROUP.length() + 1),
                    TYPE_VARIANT_DATA_TYPE);
        } else
        {
            final int nativeDataTypeId = h5.getNativeDataType(dataTypeId, fileRegistry);
            final String[] typeVariantNames = h5.getNamesForEnumOrCompoundMembers(dataTypeId);
            dataType =
                    new HDF5EnumerationType(fileId, dataTypeId, nativeDataTypeId,
                            TYPE_VARIANT_DATA_TYPE, typeVariantNames);

        }
        return dataType;
    }

    private final static int MAX_TYPE_VARIANT_TYPES = 1024;

    private String findFirstUnusedTypeVariantPath(final HDF5Reader reader)
    {
        int number = 0;
        String path;
        do
        {
            path = TYPE_VARIANT_DATA_TYPE + "." + (number++);
        } while (reader.exists(path) && number < MAX_TYPE_VARIANT_TYPES);
        return path;
    }

    private int openOrCreateVLStringType(final HDF5Reader reader)
    {
        int dataTypeId = getDataTypeId(HDF5Utils.VARIABLE_LENGTH_STRING_DATA_TYPE, reader);
        if (dataTypeId < 0)
        {
            dataTypeId = h5.createDataTypeVariableString(fileRegistry);
            commitDataType(VARIABLE_LENGTH_STRING_DATA_TYPE, dataTypeId);
        }
        return dataTypeId;
    }

    /**
     * The file will be truncated to length 0 if it already exists, that is its content will be
     * deleted.
     */
    public HDF5WriterConfig overwrite()
    {
        this.overwrite = true;
        return this;
    }

    /**
     * Use data types which can not be extended later on. This may reduce the initial size of the
     * HDF5 file.
     */
    public HDF5WriterConfig dontUseExtendableDataTypes()
    {
        this.useExtentableDataTypes = false;
        return this;
    }

    /**
     * A file will be created that uses the latest available file format. This may improve
     * performance or space consumption but in general means that older versions of the library are
     * no longer able to read this file.
     */
    public HDF5WriterConfig useLatestFileFormat()
    {
        this.useLatestFileFormat = true;
        return this;
    }

    /**
     * Will try to perform numeric conversions where appropriate if supported by the platform.
     * <p>
     * <strong>Numeric conversions can be platform dependent and are not available on all platforms.
     * Be advised not to rely on numeric conversions if you can help it!</strong>
     */
    @Override
    public HDF5WriterConfig performNumericConversions()
    {
        return (HDF5WriterConfig) super.performNumericConversions();
    }

    /**
     * Returns an {@link HDF5Writer} based on this configuration.
     */
    public HDF5Writer writer()
    {
        return new HDF5Writer(this);
    }

}
