/*
 * Copyright 2009 ETH Zuerich, CISD
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
 * The configuration of the writer is done by chaining calls to configuration methods before calling
 * {@link #writer()}.
 * 
 * @author Bernd Rinn
 */
public interface IHDF5WriterConfigurator extends IHDF5ReaderConfigurator
{
    /**
     * The mode of synchronizing changes (using a method like <code>fsync(2)</code>) to the HDF5
     * file with the underlying storage. As <code>fsync(2)</code> is blocking, the synchonization is
     * by default performed in a separate thread to minimize latency effects on the application. In
     * order to ensure that <code>fsync(2)</code> is called in the same thread, use one of the
     * <code>*_BLOCK</code> modes.
     * <p>
     * Note that non-blocking modes can have unexpected interactions with mandatory locks on
     * Windows. The symptom of that will be that the program holds a lock to the HDF5 file for some
     * (short) time even after the file has been closed. Thus, on Windows by default a blocking mode
     * is chosen.
     */
    public enum SyncMode
    {
        /**
         * Do not synchronize at all.
         */
        NO_SYNC,
        /**
         * Synchronize whenever {@link IHDF5FileLevelReadWriteHandler#flush()} or
         * {@link IHDF5FileLevelReadWriteHandler#close()} are called.
         */
        SYNC,
        /**
         * Synchronize whenever {@link IHDF5FileLevelReadWriteHandler#flush()} or
         * {@link IHDF5FileLevelReadWriteHandler#close()} are called. Block until synchronize is
         * finished.
         */
        SYNC_BLOCK,
        /**
         * Synchronize whenever {@link IHDF5FileLevelReadWriteHandler#flush()} is called. <i>Default
         * on Unix</i>
         */
        SYNC_ON_FLUSH,
        /**
         * Synchronize whenever {@link IHDF5FileLevelReadWriteHandler#flush()} is called. Block
         * until synchronize is finished. <i>Default on Windows</i>.
         */
        SYNC_ON_FLUSH_BLOCK,
    }

    /**
     * Specify file format compatibility settings.
     */
    public enum FileFormat
    {
        /**
         * Enforce compatibility with HDF5 1.6 format.
         */
        STRICTLY_1_6,

        /**
         * Start with HDF5 1.6 format, but allow usage of features which require HDF5 1.8 library to
         * read. <i>Default</i>.
         */
        ALLOW_1_8,

        /**
         * Enforce compatibility with HDF5 1.8 format.
         */
        STRICTLY_1_8;

        /**
         * Returns <code>true</code> if using HDF5 1.8 features is OK.
         */
        boolean isHDF5_1_8_OK()
        {
            return ordinal() > STRICTLY_1_6.ordinal();
        }

    }

    /**
     * The file will be truncated to length 0 if it already exists, that is its content will be
     * deleted.
     */
    public IHDF5WriterConfigurator overwrite();

    /**
     * Use data types which can not be extended later on. This may reduce the initial size of the
     * HDF5 file.
     */
    public IHDF5WriterConfigurator dontUseExtendableDataTypes();

    /**
     * Use simple data spaces for attributes.
     */
    public IHDF5WriterConfigurator useSimpleDataSpaceForAttributes();
    
    /**
     * On writing a data set, keep the data set if it exists and only write the new data. This is
     * equivalent to the <code>_KEEP</code> variants of {@link HDF5GenericStorageFeatures} and makes
     * this behavior the default.
     * <p>
     * If this setting is not given, an existing data set will be deleted before the data set is
     * written.
     * <p>
     * <i>Note:</i> If this configuration option is chosen, data types and storage features may only
     * apply if the written data set does not yet exist. For example, it may lead to a string value
     * being truncated on write if a string dataset with the same name and shorter length already
     * exists.
     */
    public IHDF5WriterConfigurator keepDataSetsIfTheyExist();

    /**
     * Sets the file format compatibility for the writer.
     */
    public IHDF5WriterConfigurator fileFormat(FileFormat newFileFormat);

    /**
     * Sets the {@link SyncMode}.
     */
    public IHDF5WriterConfigurator syncMode(SyncMode newSyncMode);

    /**
     * Will try to perform numeric conversions where appropriate if supported by the platform.
     * <p>
     * <strong>Numeric conversions can be platform dependent and are not available on all platforms.
     * Be advised not to rely on numeric conversions if you can help it!</strong>
     */
    @Override
    public IHDF5WriterConfigurator performNumericConversions();

    /**
     * Sets UTF8 character encoding for all paths and all strings in this file. (The default is
     * ASCII.)
     */
    @Override
    public IHDF5WriterConfigurator useUTF8CharacterEncoding();

    /**
     * Switches off automatic dereferencing of unresolved references. Use this when you need to
     * access file names that start with \0. The down-side of switching off automatic dereferencing
     * is that you can't provide references as obtained by
     * {@link IHDF5ReferenceReader#read(String, boolean)} with <code>resolveName=false</code> in
     * places where a dataset path is required. <br>
     * <i>Note: automatic dereferencing is switched on by default.</i>
     */
    @Override
    public IHDF5WriterConfigurator noAutoDereference();

    /**
     * Sets the suffix that is used to mark and recognize house keeping files and groups. An empty
     * string ("") encodes for the default, which is two leading and two trailing underscores
     * ("__NAME__").
     */
    public IHDF5WriterConfigurator houseKeepingNameSuffix(String houseKeepingNameSuffix);

    /**
     * Returns an {@link IHDF5Writer} based on this configuration.
     */
    public IHDF5Writer writer();

}
