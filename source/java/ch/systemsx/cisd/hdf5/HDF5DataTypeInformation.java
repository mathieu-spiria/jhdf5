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

/**
 * A class that holds relevant information about a data type.
 * 
 * @author Bernd Rinn
 */
public final class HDF5DataTypeInformation
{

    private final HDF5DataClass dataClass;

    private int elementSize;

    private int numberOfElements;

    HDF5DataTypeInformation(HDF5DataClass dataClass, int elementSize)
    {
        this(dataClass, elementSize, 1);
    }
    
    HDF5DataTypeInformation(HDF5DataClass dataClass, int elementSize, int numberOfElements)
    {
        this.dataClass = dataClass;
        this.elementSize = elementSize;
        this.numberOfElements = numberOfElements;
    }

    /**
     * Returns the data class (<code>INTEGER</code>, <code>FLOAT</code>, ...) of this type.
     */
    public HDF5DataClass getDataClass()
    {
        return dataClass;
    }

    /**
     * Returns the size of one element (in bytes) of this type.
     */
    public int getElementSize()
    {
        return elementSize;
    }

    void setElementSize(int elementSize)
    {
        this.elementSize = elementSize;
    }

    /**
     * Returns the number of elements of this type.
     * <p>
     * This will be 1 except for array data types.
     */
    public int getNumberOfElements()
    {
        return numberOfElements;
    }

    void setNumberOfElements(int numberOfElements)
    {
        this.numberOfElements = numberOfElements;
    }

    //
    // Object
    //

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || obj instanceof HDF5DataTypeInformation == false)
        {
            return false;
        }
        final HDF5DataTypeInformation that = (HDF5DataTypeInformation) obj;
        return dataClass.equals(that.dataClass) && elementSize == that.elementSize
                && numberOfElements == that.numberOfElements;
    }

    @Override
    public int hashCode()
    {
        return ((17 * 59 + dataClass.hashCode()) * 59 + elementSize) * 59 + numberOfElements;
    }

    @Override
    public String toString()
    {
        if (numberOfElements == 1)
        {
            return dataClass + "(" + elementSize + ")";
        } else
        {
            
            return dataClass + "(" + elementSize + ", #" + numberOfElements + ")";
        }
    }

}