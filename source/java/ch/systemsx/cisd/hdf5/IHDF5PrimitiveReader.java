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

/**
 * An interface that provides methods for reading primitive values from HDF5 files.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5PrimitiveReader extends IHDF5ByteReader, IHDF5DoubleReader, IHDF5FloatReader,
        IHDF5IntReader, IHDF5LongReader, IHDF5ShortReader
{
    // No methods to add.
}
