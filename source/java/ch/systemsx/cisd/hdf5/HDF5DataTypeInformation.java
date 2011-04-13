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

import org.apache.commons.lang.ObjectUtils;

import ch.systemsx.cisd.base.mdarray.MDArray;

/**
 * A class that holds relevant information about a data type.
 * 
 * @author Bernd Rinn
 */
public final class HDF5DataTypeInformation
{

    private final HDF5DataClass dataClass;

    private final boolean arrayType;

    private final String dataTypePathOrNull;

    private final String nameOrNull;

    private int elementSize;

    private int numberOfElements;

    private int[] dimensions;

    private String opaqueTagOrNull;

    HDF5DataTypeInformation(String dataTypePathOrNull, HDF5DataClass dataClass, int elementSize)
    {
        this(dataTypePathOrNull, dataClass, elementSize, new int[]
            { 1 }, false, null);
    }

    HDF5DataTypeInformation(HDF5DataClass dataClass, int elementSize)
    {
        this(null, dataClass, elementSize, new int[]
            { 1 }, false, null);
    }

    HDF5DataTypeInformation(HDF5DataClass dataClass, int elementSize, int numberOfElements)
    {
        this(null, dataClass, elementSize, new int[]
            { numberOfElements }, false, null);

    }

    HDF5DataTypeInformation(String dataTypePathOrNull, HDF5DataClass dataClass, int elementSize,
            int numberOfElements, String opaqueTagOrNull)
    {
        this(dataTypePathOrNull, dataClass, elementSize, new int[]
            { numberOfElements }, false, opaqueTagOrNull);
    }

    HDF5DataTypeInformation(String dataTypePathOrNull, HDF5DataClass dataClass, int elementSize,
            int[] dimensions, boolean arrayType)
    {
        this(dataTypePathOrNull, dataClass, elementSize, dimensions, arrayType, null);

    }

    HDF5DataTypeInformation(String dataTypePathOrNull, HDF5DataClass dataClass, int elementSize,
            int[] dimensions, boolean arrayType, String opaqueTagOrNull)
    {
        if (dataClass == HDF5DataClass.BOOLEAN || dataClass == HDF5DataClass.STRING)
        {
            this.dataTypePathOrNull = null;
            this.nameOrNull = null;
        } else
        {
            this.dataTypePathOrNull = dataTypePathOrNull;
            this.nameOrNull = HDF5Utils.tryGetDataTypeNameFromPath(dataTypePathOrNull, dataClass);
        }
        this.arrayType = arrayType;
        this.dataClass = dataClass;
        this.elementSize = elementSize;
        this.dimensions = dimensions;
        this.numberOfElements = MDArray.getLength(dimensions);
        this.opaqueTagOrNull = opaqueTagOrNull;
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

    /**
     * Returns the total size (in bytes) of this data set.
     */
    public int getSize()
    {
        return elementSize * numberOfElements;
    }

    /**
     * Returns the dimensions along each axis of this type.
     */
    public int[] getDimensions()
    {
        return dimensions;
    }

    void setDimensions(int[] dimensions)
    {
        this.dimensions = dimensions;
        this.numberOfElements = MDArray.getLength(dimensions);
    }

    public boolean isArrayType()
    {
        return arrayType;
    }

    public boolean isVariableLengthType()
    {
        return elementSize < 0;
    }

    public String tryGetOpaqueTag()
    {
        return opaqueTagOrNull;
    }

    /**
     * If this is a committed (named) data type, return the path of the data type. Otherwise
     * <code>null</code> is returned.
     */
    public String tryGetDataTypePath()
    {
        return dataTypePathOrNull;
    }

    /**
     * Returns the name of this datatype, if it is a committed data type.
     */
    public String tryGetName()
    {
        return nameOrNull;
    }

    /**
     * Returns an appropriate Java type, or <code>null</code>, if this HDF5 type has no appropriate
     * Java type.
     */
    public Class<?> tryGetJavaType()
    {
        final int rank = (dimensions.length == 1 && dimensions[0] == 1) ? 0 : dimensions.length;
        return dataClass.getJavaTypeProvider().getJavaType(rank, elementSize);
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
                && numberOfElements == that.numberOfElements
                && ObjectUtils.equals(nameOrNull, that.nameOrNull)
                && ObjectUtils.equals(dataTypePathOrNull, that.dataTypePathOrNull);
    }

    @Override
    public int hashCode()
    {
        return ((((17 * 59 + dataClass.hashCode()) * 59 + elementSize) * 59 + numberOfElements) * 59 + ObjectUtils
                .hashCode(nameOrNull)) * 59 + ObjectUtils.hashCode(dataTypePathOrNull);
    }

    @Override
    public String toString()
    {
        final String name;
        if (nameOrNull != null)
        {
            name = "{" + nameOrNull + "}";
        } else
        {
            name = "";
        }
        if (numberOfElements == 1)
        {
            return name + dataClass + "(" + elementSize + ")";
        } else if (dimensions.length == 1)
        {

            return name + dataClass + "(" + elementSize + ", #" + numberOfElements + ")";
        } else
        {
            final StringBuilder builder = new StringBuilder();
            builder.append(name);
            builder.append(dataClass.toString());
            builder.append('(');
            builder.append(elementSize);
            builder.append(", [");
            for (int d : dimensions)
            {
                builder.append(d);
                builder.append(',');
            }
            if (dimensions.length > 0)
            {
                builder.setLength(builder.length() - 1);
            }
            builder.append(']');
            builder.append(')');
            return builder.toString();
        }
    }
}