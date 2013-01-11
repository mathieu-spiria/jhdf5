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

import ch.systemsx.cisd.base.mdarray.MDLongArray;

/**
 * A multi-dimensional array of time durations.
 * 
 * @author Bernd Rinn
 */
public class HDF5TimeDurationMDArray
{
    final MDLongArray timeDurations;

    final HDF5TimeUnit timeUnit;

    /**
     * Creates an array of <var>timeDurations</var> using a common <var>timeUnit</var>.
     */
    public HDF5TimeDurationMDArray(MDLongArray timeDurations, HDF5TimeUnit timeUnit)
    {
        this.timeDurations = timeDurations;
        this.timeUnit = timeUnit;
    }

    /**
     * Creates an array of <var>timeDurations</var> using a common <var>timeUnit</var>.
     */
    public HDF5TimeDurationMDArray(long[] timeDurations, int[] dimensions, HDF5TimeUnit timeUnit)
    {
        this.timeDurations = new MDLongArray(timeDurations, dimensions, true);
        this.timeUnit = timeUnit;
    }

    /**
     * Creates an array of <var>timeDurations</var> using a common <var>timeUnit</var>.
     */
    public HDF5TimeDurationMDArray(HDF5TimeDuration[] timeDurations, int[] dimensions,
            HDF5TimeUnit timeUnit)
    {
        HDF5TimeUnit smallestTimeUnit = getSmallestUnit(timeDurations);
        final long[] durations = new long[timeDurations.length];
        if (timeUnit != smallestTimeUnit)
        {
            for (int i = 0; i < timeDurations.length; ++i)
            {
                durations[i] = timeUnit.convert(timeDurations[i]);
            }
        } else
        {
            for (int i = 0; i < timeDurations.length; ++i)
            {
                durations[i] = timeDurations[i].getValue();
            }
        }
        this.timeDurations = new MDLongArray(durations, dimensions, true);
        this.timeUnit = timeUnit;
    }

    /**
     * Creates an array of <var>timeDurations</var> using the smallest time unit.
     */
    public HDF5TimeDurationMDArray(HDF5TimeDuration[] timeDurations, int[] dimensions)
    {
        HDF5TimeUnit smallestTimeUnit = getSmallestUnit(timeDurations);
        final long[] durations = new long[timeDurations.length];
        for (int i = 0; i < timeDurations.length; ++i)
        {
            durations[i] = smallestTimeUnit.convert(timeDurations[i]);
        }
        this.timeDurations = new MDLongArray(durations, dimensions, true);
        this.timeUnit = smallestTimeUnit;
    }

    private static HDF5TimeUnit getSmallestUnit(HDF5TimeDuration[] timeDurations)
    {
        HDF5TimeUnit unit = timeDurations[0].getUnit();
        for (int i = 1; i < timeDurations.length; ++i)
        {
            final HDF5TimeUnit u = timeDurations[i].getUnit();
            if (u != unit)
            {
                if (u.ordinal() < unit.ordinal())
                {
                    unit = u;
                }
            }
        }
        return unit;
    }

    /**
     * Returns the time unit.
     */
    public HDF5TimeUnit getUnit()
    {
        return timeUnit;
    }

    /**
     * Returns the time duration values.
     */
    public MDLongArray getValues()
    {
        return timeDurations;
    }

    /**
     * Returns the number of elements.
     */
    public int[] getDimensions()
    {
        return timeDurations.dimensions();
    }

    /**
     * Returns the number of elements.
     */
    public int getLength()
    {
        return timeDurations.size();
    }

    /**
     * Returns the time duration values in the given <var>targetUnit</var>.
     */
    public MDLongArray getValues(HDF5TimeUnit targetUnit)
    {
        if (targetUnit == timeUnit)
        {
            return timeDurations;
        }
        final long[] sourceDurations = timeDurations.getAsFlatArray();
        final long[] targetDurations = new long[sourceDurations.length];
        for (int i = 0; i < targetDurations.length; ++i)
        {
            targetDurations[i] = targetUnit.convert(sourceDurations[i], timeUnit);
        }
        return new MDLongArray(targetDurations, timeDurations.dimensions());
    }

    /**
     * Returns the value of array at the position defined by <var>indices</var>.
     */
    public HDF5TimeDuration get(int... indices)
    {
        return new HDF5TimeDuration(timeDurations.get(indices), timeUnit);
    }

    /**
     * Returns the value of a one-dimensional array at the position defined by <var>index</var>.
     * <p>
     * <b>Do not call for arrays other than one-dimensional!</b>
     */
    public HDF5TimeDuration get(int index)
    {
        return new HDF5TimeDuration(timeDurations.get(index), timeUnit);
    }

    /**
     * Returns the value of a two-dimensional array at the position defined by <var>indexX</var> and
     * <var>indexY</var>.
     * <p>
     * <b>Do not call for arrays other than two-dimensional!</b>
     */
    public HDF5TimeDuration get(int indexX, int indexY)
    {
        return new HDF5TimeDuration(timeDurations.get(indexX, indexY), timeUnit);
    }

    /**
     * Returns the value of a three-dimensional array at the position defined by <var>indexX</var>,
     * <var>indexY</var> and <var>indexZ</var>.
     * <p>
     * <b>Do not call for arrays other than three-dimensional!</b>
     */
    public HDF5TimeDuration get(int indexX, int indexY, int indexZ)
    {
        return new HDF5TimeDuration(timeDurations.get(indexX, indexY, indexZ), timeUnit);
    }

    /**
     * Returns the value of a one-dimensional array at the position defined by <var>index</var> in
     * the given <var>targetUnit</var>.
     * <p>
     * <b>Do not call for arrays other than one-dimensional!</b>
     */
    public HDF5TimeDuration get(HDF5TimeUnit targetUnit, int index)
    {
        if (targetUnit == timeUnit)
        {
            return new HDF5TimeDuration(timeDurations.get(index), timeUnit);
        } else
        {
            return new HDF5TimeDuration(targetUnit.convert(timeDurations.get(index), timeUnit),
                    targetUnit);
        }
    }

    /**
     * Returns the value of a two-dimensional array at the position defined by <var>indexX</var> and
     * <var>indexY</var> in the given <var>targetUnit</var>.
     * <p>
     * <b>Do not call for arrays other than two-dimensional!</b>
     */
    public HDF5TimeDuration get(HDF5TimeUnit targetUnit, int indexX, int indexY)
    {
        if (targetUnit == timeUnit)
        {
            return new HDF5TimeDuration(timeDurations.get(indexX, indexY), timeUnit);
        } else
        {
            return new HDF5TimeDuration(targetUnit.convert(timeDurations.get(indexX, indexY),
                    timeUnit), targetUnit);
        }
    }

    /**
     * Returns the value of a three-dimensional array at the position defined by <var>indexX</var>,
     * <var>indexY</var> and <var>indexZ</var> in the given <var>targetUnit</var>.
     * <p>
     * <b>Do not call for arrays other than three-dimensional!</b>
     */
    public HDF5TimeDuration get(HDF5TimeUnit targetUnit, int indexX, int indexY, int indexZ)
    {
        if (targetUnit == timeUnit)
        {
            return new HDF5TimeDuration(timeDurations.get(indexX, indexY, indexZ), timeUnit);
        } else
        {
            return new HDF5TimeDuration(targetUnit.convert(
                    timeDurations.get(indexX, indexY, indexZ), timeUnit), targetUnit);
        }
    }

    /**
     * Returns the value of array at the position defined by <var>indices</var> in the given
     * <var>targetUnit</var>.
     */
    public HDF5TimeDuration get(HDF5TimeUnit targetUnit, int... indices)
    {
        if (targetUnit == timeUnit)
        {
            return new HDF5TimeDuration(timeDurations.get(indices), timeUnit);
        } else
        {
            return new HDF5TimeDuration(targetUnit.convert(timeDurations.get(indices), timeUnit),
                    targetUnit);
        }
    }

    /**
     * Returns the value element <var>index</var>.
     * <p>
     * <b>Do not call for arrays other than one-dimensional!</b>
     */
    public long getValue(int index)
    {
        return timeDurations.get(index);
    }

    /**
     * Returns the value element <var>(indexX,indexY)</var>.
     * <p>
     * <b>Do not call for arrays other than two-dimensional!</b>
     */
    public long getValue(int indexX, int indexY)
    {
        return timeDurations.get(indexX, indexY);
    }

    /**
     * Returns the value element <var>(indexX,indexY,indexZ)</var>.
     * <p>
     * <b>Do not call for arrays other than three-dimensional!</b>
     */
    public long getValue(int indexX, int indexY, int indexZ)
    {
        return timeDurations.get(indexX, indexY, indexZ);
    }

    /**
     * Returns the value element <var>indices</var>.
     */
    public long getValue(int... indices)
    {
        return timeDurations.get(indices);
    }

    /**
     * Returns the value element <var>index</var> in the given <var>targetUnit</var>.
     * <p>
     * <b>Do not call for arrays other than one-dimensional!</b>
     */
    public long getValue(HDF5TimeUnit targetUnit, int index)
    {
        return (targetUnit == timeUnit) ? timeDurations.get(index) : targetUnit.convert(
                timeDurations.get(index), timeUnit);
    }

    /**
     * Returns the value element <var>index</var> in the given <var>targetUnit</var>.
     * <p>
     * <b>Do not call for arrays other than one-dimensional!</b>
     */
    public long getValue(HDF5TimeUnit targetUnit, int indexX, int indexY)
    {
        return (targetUnit == timeUnit) ? timeDurations.get(indexX, indexY) : targetUnit.convert(
                timeDurations.get(indexX, indexY), timeUnit);
    }

    /**
     * Returns the value element <var>index</var> in the given <var>targetUnit</var>.
     * <p>
     * <b>Do not call for arrays other than one-dimensional!</b>
     */
    public long getValue(HDF5TimeUnit targetUnit, int indexX, int indexY, int indexZ)
    {
        return (targetUnit == timeUnit) ? timeDurations.get(indexX, indexY, indexZ) : targetUnit
                .convert(timeDurations.get(indexX, indexY, indexZ), timeUnit);
    }

    /**
     * Returns the value element <var>index</var> in the given <var>targetUnit</var>.
     * <p>
     * <b>Do not call for arrays other than one-dimensional!</b>
     */
    public long getValue(HDF5TimeUnit targetUnit, int... indices)
    {
        return (targetUnit == timeUnit) ? timeDurations.get(indices) : targetUnit.convert(
                timeDurations.get(indices), timeUnit);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + timeDurations.hashCode();
        result = prime * result + ((timeUnit == null) ? 0 : timeUnit.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        final HDF5TimeDurationMDArray other = (HDF5TimeDurationMDArray) obj;
        if (timeDurations.equals(other.timeDurations) == false)
        {
            return false;
        }
        if (timeUnit != other.timeUnit)
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "HDF5TimeDurationArray [timeDurations=" + timeDurations.toString() + ", timeUnit="
                + timeUnit + "]";
    }
}