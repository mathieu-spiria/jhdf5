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
 * An interface for writing HDF5 files (HDF5 1.6.x or HDF5 1.8.x).
 * <p>
 * The interface focuses on ease of use instead of completeness. As a consequence not all valid HDF5
 * files can be generated using this class, but only a subset.
 * <p>
 * Usage:
 * 
 * <pre>
 * float[] f = new float[100];
 * ...
 * IHDF5Writer writer = HDF5FactoryProvider.get().open(new File(&quot;test.h5&quot;));
 * writer.writeFloatArray(&quot;/some/path/dataset&quot;, f);
 * writer.setStringAttribute(&quot;some key&quot;, &quot;some value&quot;);
 * writer.close();
 * </pre>
 * 
 * @author Bernd Rinn
 */
@SuppressWarnings("deprecation")
public interface IHDF5Writer extends IHDF5Reader, IHDF5SimpleWriter, IHDF5LegacyWriter
{
    // /////////////////////
    // File
    // /////////////////////

    /**
     * Returns the handler for file-level information and status.
     */
    @Override
    public IHDF5FileLevelReadWriteHandler file();

    // /////////////////////////////////
    // Objects, links, groups and types
    // /////////////////////////////////

    /**
     * Returns an info provider and handler for HDF5 objects like links, groups, data sets and data
     * types.
     */
    @Override
    public IHDF5ObjectReadWriteInfoProviderHandler object();

    // /////////////////////
    // Opaque
    // /////////////////////

    /**
     * Returns the full writer for opaque values.
     */
    @Override
    public IHDF5OpaqueWriter opaque();

    // /////////////////////
    // Boolean
    // /////////////////////

    /**
     * Returns the full writer for boolean values.
     */
    @Override
    public IHDF5BooleanWriter bool();

    // /////////////////////
    // Bytes
    // /////////////////////

    /**
     * Returns the full writer for byte / int8.
     */
    @Override
    public IHDF5ByteWriter int8();

    /**
     * Returns the full writer for unsigned byte / uint8.
     */
    public IHDF5UnsignedByteWriter uint8();

    // /////////////////////
    // Short
    // /////////////////////

    /**
     * Returns the full writer for short / int16.
     */
    @Override
    public IHDF5ShortWriter int16();

    /**
     * Returns the full writer for unsigned short / uint16.
     */
    public IHDF5UnsignedShortWriter uint16();

    // /////////////////////
    // Int
    // /////////////////////

    /**
     * Returns the full writer for int / int32.
     */
    @Override
    public IHDF5IntWriter int32();

    /**
     * Returns the full writer for unsigned int / uint32.
     */
    public IHDF5UnsignedIntWriter uint32();

    // /////////////////////
    // Long
    // /////////////////////

    /**
     * Returns the full writer for long / int64.
     */
    @Override
    public IHDF5LongWriter int64();

    /**
     * Returns the full writer for unsigned long / uint64.
     */
    public IHDF5UnsignedLongWriter uint64();

    // /////////////////////
    // Float
    // /////////////////////

    /**
     * Returns the full writer for float / float32.
     */
    @Override
    public IHDF5FloatWriter float32();

    // /////////////////////
    // Double
    // /////////////////////

    /**
     * Returns the full writer for long / float64.
     */
    @Override
    public IHDF5DoubleWriter float64();

    // /////////////////////
    // Enums
    // /////////////////////

    /**
     * Returns the full writer for enums.
     * 
     * @deprecated Use {@link #enumeration()} instead.
     */
    @Deprecated
    @Override
    public IHDF5EnumWriter enums();

    /**
     * Returns the full writer for enumerations.
     */
    @Override
    public IHDF5EnumWriter enumeration();

    // /////////////////////
    // Compounds
    // /////////////////////

    /**
     * Returns the full writer for compounds.
     * 
     * @deprecated Use {@link #compound()} instead.
     */
    @Deprecated
    @Override
    public IHDF5CompoundWriter compounds();

    /**
     * Returns the full reader for compounds.
     */
    @Override
    public IHDF5CompoundWriter compound();

    // /////////////////////
    // Strings
    // /////////////////////

    /**
     * Returns the full writer for strings.
     */
    @Override
    public IHDF5StringWriter string();

    // /////////////////////
    // Date & Time
    // /////////////////////

    /**
     * Returns the full writer for date and times.
     */
    @Override
    public IHDF5DateTimeWriter time();

    /**
     * Returns the full writer for time durations.
     */
    @Override
    public IHDF5TimeDurationWriter duration();

    // /////////////////////
    // Object references
    // /////////////////////

    /**
     * Returns the full reader for object references.
     */
    @Override
    public IHDF5ReferenceWriter reference();

}
