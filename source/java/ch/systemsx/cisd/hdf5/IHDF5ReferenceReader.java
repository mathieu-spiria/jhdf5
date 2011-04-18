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

import ch.systemsx.cisd.base.mdarray.MDArray;

/**
 * An interface for reading references in HDF5 files.
 * 
 * @see IHDF5ReferenceWriter
 * @author Bernd Rinn
 */
public interface IHDF5ReferenceReader
{

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Reads an object reference attribute named <var>attributeName</var> from the object
     * <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param attributeName The name of the attribute to read.
     * @return The path of the object that the reference refers to, or an empty string, if the
     *         object reference refers to an unnamed object.
     */
    public String getObjectReferenceAttribute(final String objectPath, final String attributeName);

    public String[] getObjectReferenceArrayAttribute(final String objectPath,
            final String attributeName);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Reads an object reference from the object <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The path of the object that the reference refers to, or an empty string, if the
     *         object reference refers to an unnamed object.
     */
    public String readObjectReference(final String objectPath);

    /**
     * Reads an array of object references from the object <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The array of the paths of objects that the references refers to. Each string may be
     *         empty, if the correspondig object reference refers to an unnamed object.
     */
    public String[] readObjectReferenceArray(final String objectPath);

    /**
     * Reads an array (or rank N) of object references from the object <var>objectPath</var>.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @return The multi-dimensionsl array of the paths of objects that the references refers to.
     *         Each string may be empty, if the correspondig object reference refers to an unnamed
     *         object.
     */
    public MDArray<String> readObjectReferenceMDArray(final String objectPath);
}
