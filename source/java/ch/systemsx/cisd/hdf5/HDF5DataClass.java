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

import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_ARRAY;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_BITFIELD;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_COMPOUND;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_ENUM;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_FLOAT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_INTEGER;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_OPAQUE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_STRING;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5T_VLEN;

/**
 * Identifies the class of a data type. Note that for array types the class of the elements is
 * identified.
 * 
 * @author Bernd Rinn
 */
public enum HDF5DataClass
{
    ARRAY(H5T_ARRAY), BITFIELD(H5T_BITFIELD), BOOLEAN(-1), COMPOUND(H5T_COMPOUND), ENUM(H5T_ENUM),
    FLOAT(H5T_FLOAT), INTEGER(H5T_INTEGER), OPAQUE(H5T_OPAQUE), STRING(H5T_STRING), VLEN(H5T_VLEN),
    OTHER(-1);

    private int id;

    HDF5DataClass(int id)
    {
        this.id = id;
    }

    /**
     * Returns the {@link HDF5DataClass} for the given data <var>classId</var>.
     * <p>
     * <b>Note:</b> This method will never return {@link #BOOLEAN}, but instead it will return
     * {@link #ENUM} for a boolean value as boolean values are actually enums in the HDF5 file.
     */
    static HDF5DataClass classIdToDataClass(final int classId)
    {
        for (HDF5DataClass clazz : values())
        {
            if (clazz.id == classId)
            {
                return clazz;
            }
        }
        return OTHER;
    }

}