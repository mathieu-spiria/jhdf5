/*
 * Copyright 2010 ETH Zuerich, CISD
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

import static ncsa.hdf.hdf5lib.H5.H5Dwrite_long;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_ALL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_NATIVE_B64;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STD_B64LE;

import java.util.BitSet;

import ncsa.hdf.hdf5lib.HDFNativeData;

import ch.systemsx.cisd.hdf5.cleanup.ICallableWithCleanUp;
import ch.systemsx.cisd.hdf5.cleanup.ICleanUpRegistry;

/**
 * Implementation of {@link IHDF5BooleanWriter}.
 *
 * @author Bernd Rinn
 */
public class HDF5BooleanWriter implements IHDF5BooleanWriter
{
    private final HDF5BaseWriter baseWriter;

    HDF5BooleanWriter(HDF5BaseWriter baseWriter)
    {
        assert baseWriter != null;

        this.baseWriter = baseWriter;
    }

    // /////////////////////
    // Attributes
    // /////////////////////

    public void setBooleanAttribute(final String objectPath, final String name, final boolean value)
    {
        baseWriter.checkOpen();
        baseWriter.setAttribute(objectPath, name, baseWriter.booleanDataTypeId,
                baseWriter.booleanDataTypeId, new byte[]
                    { (byte) (value ? 1 : 0) });
    }

    // /////////////////////
    // Data Sets
    // /////////////////////

    public void writeBoolean(final String objectPath, final boolean value)
    {
        baseWriter.checkOpen();
        baseWriter.writeScalar(objectPath, baseWriter.booleanDataTypeId,
                baseWriter.booleanDataTypeId, HDFNativeData.byteToByte((byte) (value ? 1 : 0)));
    }

    public void writeBitField(final String objectPath, final BitSet data)
    {
        writeBitField(objectPath, data, HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION);
    }

    public void writeBitField(final String objectPath, final BitSet data,
            final HDF5GenericStorageFeatures features)
    {
        assert objectPath != null;
        assert data != null;

        baseWriter.checkOpen();
        final ICallableWithCleanUp<Void> writeRunnable = new ICallableWithCleanUp<Void>()
            {
                public Void call(ICleanUpRegistry registry)
                {
                    final int longBytes = 8;
                    final int longBits = longBytes * 8;
                    final int msb = data.length();
                    final int realLength = msb / longBits + (msb % longBits != 0 ? 1 : 0);
                    final int dataSetId =
                            baseWriter.getDataSetId(objectPath, H5T_STD_B64LE, new long[]
                                { realLength }, longBytes, features, registry);
                    H5Dwrite_long(dataSetId, H5T_NATIVE_B64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                            BitSetConversionUtils.toStorageForm(data));
                    return null; // Nothing to return.
                }
            };
        baseWriter.runner.call(writeRunnable);
    }

}
