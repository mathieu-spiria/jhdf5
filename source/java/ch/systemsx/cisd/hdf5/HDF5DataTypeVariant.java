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
    // be interpreted wrongly!
    //
    // Appending of new type variants at the end of the list is fine.
    //

    /**
     * Used for data sets that encode time stamps as number of milli-seconds since midnight, January
     * 1, 1970 UTC (aka "start of the epoch").
     */
    TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH,

    /**
     * A time duration in micro-seconds.
     */
    TIME_DURATION_MICROSECONDS,

    /**
     * A time duration in milli-seconds.
     */
    TIME_DURATION_MILLISECONDS,

    /**
     * A time duration in seconds.
     */
    TIME_DURATION_SECONDS,

    /**
     * A time duration in minutes.
     */
    TIME_DURATION_MINUTES,

    /**
     * A time duration in hours.
     */
    TIME_DURATION_HOURS,

    /**
     * A time duration in days.
     */
    TIME_DURATION_DAYS,
    
    /**
     * An enumeration.
     */
    ENUM;
    
    /**
     * Returns <code>true</code>, if the type variant denoted by <var>typeVariantOrdinal</var>
     * corresponds to a time duration.
     */
    public static boolean isTimeDuration(final int typeVariantOrdinal)
    {
        return typeVariantOrdinal >= TIME_DURATION_MICROSECONDS.ordinal()
                && typeVariantOrdinal <= TIME_DURATION_DAYS.ordinal();
    }

    /**
     * Returns the time unit for the given <var>typeVariantOrdinal</var>. Note that it is an error
     * if <var>typeVariantOrdinal</var> does not correspond to a time unit.
     */
    public static HDF5TimeUnit getTimeUnit(final int typeVariantOrdinal)
    {
        return HDF5TimeUnit.values()[typeVariantOrdinal
                - HDF5DataTypeVariant.TIME_DURATION_MICROSECONDS.ordinal()];
    }

    /**
     * Returns <code>true</code>, if this type variant corresponds to a time stamp.
     */
    public boolean isTimeStamp()
    {
        return this == TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH;
    }

    /**
     * Returns <code>true</code>, if this type variant corresponds to a time duration.
     */
    public boolean isTimeDuration()
    {
        return isTimeDuration(ordinal());
    }

    /**
     * Returns the time unit for this type variant or <code>null</code>, if this type variant is not
     * a time unit.
     */
    public HDF5TimeUnit tryGetTimeUnit()
    {
        final int ordinal = ordinal();
        return isTimeDuration(ordinal) ? getTimeUnit(ordinal) : null;
    }

}
