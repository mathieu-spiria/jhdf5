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

import java.io.File;

/**
 * If you want the reader to perform numeric conversions, call {@link #performNumericConversions()}
 * before calling {@link #reader()}.
 * 
 * @author Bernd Rinn
 */
class HDF5ReaderConfigurator implements IHDF5ReaderConfigurator
{

    protected final File hdf5File;

    protected boolean performNumericConversions;

    protected HDF5Reader readerWriterOrNull;

    HDF5ReaderConfigurator(File hdf5File)
    {
        assert hdf5File != null;

        this.hdf5File = hdf5File.getAbsoluteFile();
    }

    public boolean platformSupportsNumericConversions()
    {
        // Note: code in here any known exceptions of platforms not supporting numeric conversions.
        return true;
    }

    public HDF5ReaderConfigurator performNumericConversions()
    {
        if (platformSupportsNumericConversions() == false)
        {
            return this;
        }
        this.performNumericConversions = true;
        return this;
    }

    public IHDF5Reader reader()
    {
        if (readerWriterOrNull == null)
        {
            readerWriterOrNull =
                    new HDF5Reader(new HDF5BaseReader(hdf5File, performNumericConversions,
                            IHDF5WriterConfigurator.FileFormat.ALLOW_1_8, false));
        }
        return readerWriterOrNull;
    }

}
