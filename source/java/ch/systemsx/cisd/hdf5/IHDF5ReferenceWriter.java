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
 * An interface for writing references. References can refer to objects or regions of datasets. This
 * version only supports object references.
 * <p>
 * <b>Note:</b> References are a low-level feature and it is easy to get dangling or even wrong
 * references by using them. If you have a choice, don't use them, but use links instead. If you
 * have to use them, e.g. to comply with a pre-defined format definition, use them with care. The
 * most important fact to know about references is that they don't keep an object alive. Once the
 * last link to the object is gone, the object is gone as well. The reference will be
 * <i>dangling</i>. If, at a later time, another object header is written to the same place in the
 * file, the reference will refer to this new object, which is most likely an undesired effect
 * (<i>wrong reference</i>). By default JHDF5 itself deletes existing datasets before writing new
 * content to a dataset of the same name, which may lead to the described problem of dangling or
 * wrong references without any explicit call to {@link IHDF5Writer#delete(String)}. Thus, HDF5
 * files with references should always be opened for writing using the
 * {@link IHDF5WriterConfigurator#keepDataSetsIfTheyExist()} setting.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5ReferenceWriter
{

    // /////////////////////
    // Attributes
    // /////////////////////

    /**
     * Sets an object reference attribute to the referenced object.
     * <p>
     * Both the object referenced with <var>objectPath</var> and <var>referencedObjectPath</var>
     * must exist, that is it need to have been written before by one of the <code>write()</code> or
     * <code>create()</code> methods.
     * 
     * @param objectPath The name of the object to add the attribute to.
     * @param name The name of the attribute.
     * @param referencedObjectPath The path of the object to reference.
     */
    public void setObjectReferenceAttribute(final String objectPath, final String name,
            final String referencedObjectPath);

    public void setObjectReferenceArrayAttribute(final String objectPath, final String name,
            final String[] value);

    // /////////////////////
    // Data Sets
    // /////////////////////

    /**
     * Writes an object reference to the referenced object.
     * <p>
     * The object referenced with <var>referencedObjectPath</var> must exist, that is it need to
     * have been written before by one of the <code>write()</code> or <code>create()</code> methods.
     * 
     * @param objectPath The name of the object to write.
     * @param referencedObjectPath The path of the object to reference.
     */
    public void writeObjectReference(String objectPath, String referencedObjectPath);

    /**
     * Writes an array (of rank 1) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPath The names of the object to write.
     */
    public void writeObjectReferenceArray(final String objectPath,
            final String[] referencedObjectPath);

    /**
     * Writes an array (of rank 1) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPath The names of the object to write.
     * @param features The storage features of the data set.
     */
    public void writeObjectReferenceArray(final String objectPath,
            final String[] referencedObjectPath, final HDF5IntStorageFeatures features);

    /**
     * Writes an array (of rank N) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPaths The names of the object to write.
     */
    public void writeObjectReferenceMDArray(final String objectPath,
            final MDArray<String> referencedObjectPaths);

    /**
     * Writes an array (of rank N) of object references.
     * 
     * @param objectPath The name (including path information) of the data set object in the file.
     * @param referencedObjectPaths The names of the object to write.
     * @param features The storage features of the data set.
     */
    public void writeObjectReferenceMDArray(final String objectPath,
            final MDArray<String> referencedObjectPaths, final HDF5IntStorageFeatures features);
}
