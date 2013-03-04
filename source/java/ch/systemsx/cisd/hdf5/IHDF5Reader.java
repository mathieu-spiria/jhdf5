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
 * An interface for reading HDF5 files (HDF5 1.8.x and older).
 * <p>
 * The interface focuses on ease of use instead of completeness. As a consequence not all features
 * of a valid HDF5 files can be read using this class, but only a subset. (All information written
 * by {@link IHDF5Writer} can be read by this class.)
 * <p>
 * Usage:
 * 
 * <pre>
 * IHDF5Reader reader = HDF5FactoryProvider.get().openForReading(new File(&quot;test.h5&quot;));
 * float[] f = reader.readFloatArray(&quot;/some/path/dataset&quot;);
 * String s = reader.getStringAttribute(&quot;/some/path/dataset&quot;, &quot;some key&quot;);
 * reader.close();
 * </pre>
 * 
 * @author Bernd Rinn
 */
@SuppressWarnings("deprecation")
public interface IHDF5Reader extends IHDF5SimpleReader, IHDF5LegacyReader
{

    // /////////////////////
    // File
    // /////////////////////

    /**
     * Returns the handler for file-level information and status.
     */
    public IHDF5FileLevelReadOnlyHandler file();

    // /////////////////////////////////
    // Objects, links, groups and types
    // /////////////////////////////////

    /**
     * Returns an info provider for HDF5 objects like links, groups, data sets and data types. 
     */
    public IHDF5ObjectReadOnlyInfoProviderHandler object();

    // /////////////////////
    // Opaque
    // /////////////////////

    /**
     * Returns the full reader for reading data sets and attributes as byte arrays ('opaque') and
     * obtaining opaque types.
     */
    public IHDF5OpaqueReader opaque();

    // /////////////////////
    // Boolean
    // /////////////////////

    /**
     * Returns the full reader for boolean values.
     */
    public IHDF5BooleanReader bool();

    // /////////////////////
    // Bytes
    // /////////////////////

    /**
     * Returns the full reader for byte / int8.
     */
    public IHDF5ByteReader int8();

    // /////////////////////
    // Short
    // /////////////////////

    /**
     * Returns the full reader for short / int16.
     */
    public IHDF5ShortReader int16();

    // /////////////////////
    // Int
    // /////////////////////

    /**
     * Returns the full reader for int / int32.
     */
    public IHDF5IntReader int32();

    // /////////////////////
    // Long
    // /////////////////////

    /**
     * Returns the full reader for long / int64.
     */
    public IHDF5LongReader int64();

    // /////////////////////
    // Float
    // /////////////////////

    /**
     * Returns the full reader for float / float32.
     */
    public IHDF5FloatReader float32();

    // /////////////////////
    // Double
    // /////////////////////

    /**
     * Returns the full reader for long / float64.
     */
    public IHDF5DoubleReader float64();

    // /////////////////////
    // Enums
    // /////////////////////

    /**
     * Returns the full reader for enums.
     * 
     * @deprecated Use {@link #enumeration()} instead.
     */
    @Deprecated
    public IHDF5EnumReader enums();

    /**
     * Returns the full reader for enumerations.
     */
    public IHDF5EnumReader enumeration();

    // /////////////////////
    // Compounds
    // /////////////////////

    /**
     * Returns the full reader for compounds.
     * 
     * @deprecated Use {@link #compound()} instead.
     */
    @Deprecated
    public IHDF5CompoundReader compounds();

    /**
     * Returns the full reader for compounds.
     */
    public IHDF5CompoundReader compound();

    // /////////////////////
    // Strings
    // /////////////////////

    /**
     * Returns the full reader for strings.
     */
    public IHDF5StringReader string();

    // /////////////////////
    // Date & Time
    // /////////////////////

    /**
     * Returns the full reader for date and times.
     */
    public IHDF5DateTimeReader time();

    /**
     * Returns the full reader for time durations.
     */
    public IHDF5TimeDurationReader duration();

    // /////////////////////
    // Object references
    // /////////////////////

    /**
     * Returns the full reader for object references.
     */
    public IHDF5ReferenceReader reference();

}
