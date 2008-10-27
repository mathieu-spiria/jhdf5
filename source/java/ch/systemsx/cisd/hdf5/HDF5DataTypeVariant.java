/*
 * Copyright 2008 ETH Zuerich, CISD
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
 * An enum of all type variants. Type variants contain additional information on how to interpret a
 * data set, similar to the tag for the opaque type.
 * 
 * @author Bernd Rinn
 */
public enum HDF5DataTypeVariant
{
    //
    // Implementation note: Never change the order or the names of the values or else old files will
    // be read wrong! Additions at the end of the lists are fine.
    //

    /**
     * Used for data sets that encode time stamps as number of milli-seconds since midnight, January
     * 1, 1970 UTC (aka "start of the epoch").
     */
    TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,
}
