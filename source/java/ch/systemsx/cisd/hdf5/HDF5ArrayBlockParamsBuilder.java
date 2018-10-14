/*
 * Copyright 2007 - 2018 ETH Zuerich, CISD and SIS.
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
 * A builder class for {@link HDF5ArrayBlockParams}.
 */
public class HDF5ArrayBlockParamsBuilder
{
    /**
     * Creates a parameter that reads the complete array.
     */
    public static HDF5ArrayBlockParams array()
    {
        return new HDF5ArrayBlockParams();
    }

    /**
     * Creates a parameter that reads an array block.
     * 
     * @param dimensions The block dimensions.
     */
    public static HDF5ArrayBlockParams block(int... dimensions)
    {
        final HDF5ArrayBlockParams params = new HDF5ArrayBlockParams();
        params.blockDimensions = dimensions;
        return params;
    }

    /**
     * Creates a parameter that reads an array slice.
     * 
     * @param boundIndices The array containing the values of the bound indices at the respective
     *            index positions, and -1 at the free index positions. For example an array of
     *            <code>new long[] { -1, -1, 5, -1, 7, -1 }</code> has 2 and 4 as bound indices and
     *            binds them to the values 5 and 7, respectively.
     */
    public static HDF5ArrayBlockParams slice(long... boundIndices)
    {
        final HDF5ArrayBlockParams params = new HDF5ArrayBlockParams();
        params.boundIndexArray = boundIndices;
        return params;
    }

    /**
     * Creates a parameter that reads an array slice.
     * 
     * @param boundIndices The mapping of indices to index values which should be bound. For example
     *            a map of <code>new IndexMap().mapTo(2, 5).mapTo(4, 7)</code> has 2 and 4 as bound
     *            indices and binds them to the values 5 and 7, respectively.
     */
    public static HDF5ArrayBlockParams slice(IndexMap boundIndices)
    {
        final HDF5ArrayBlockParams params = new HDF5ArrayBlockParams();
        params.boundIndexMap = boundIndices;
        return params;
    }

}
