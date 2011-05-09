/*
 * Copyright 2011 ETH Zuerich, CISD
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
 * An object to store a time duration.
 *
 * @author Bernd Rinn
 */
public class HDF5TimeDuration
{
    private final long duration;
    
    private final HDF5TimeUnit unit;
    
    public HDF5TimeDuration(long duration, HDF5TimeUnit unit)
    {
        this.duration = duration;
        this.unit = unit;
    }

    /**
     * The time duration, see {@link #getUnit()} for the time unit.
     */
    public long getDuration()
    {
        return duration;
    }

    /**
     * The time unit of the duration.
     */
    public HDF5TimeUnit getUnit()
    {
        return unit;
    }

    /**
     * Returns <code>true</code>, if <var>that</var> represents the same time duration.
     */
    public boolean isEquivalent(HDF5TimeDuration that)
    {
        if (this.unit == that.unit)
        {
            return this.duration == that.duration;
        } else
        {
            return this.unit.convert(that) == this.duration;
        }
    }
    
    //
    // Object
    //
    
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (duration ^ (duration >>> 32));
        result = prime * result + ((unit == null) ? 0 : unit.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HDF5TimeDuration other = (HDF5TimeDuration) obj;
        if (duration != other.duration)
            return false;
        if (unit != other.unit)
            return false;
        return true;
    }
    
    @Override
    public String toString()
    {
        return Long.toString(duration) + " " + unit.toString();
    }

}
