/*
 * Copyright 2007 - 2014 ETH Zuerich, CISD and SIS.
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

package ch.systemsx.cisd.hdf5.hdf5lib;

import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Low-level interface for HDF5 datatype functions.
 * <p>
 * <b>This is an internal API that should not be expected to be stable between releases!</b>
 * 
 * @author Bernd Rinn
 */
public class H5T
{
    static
    {
        H5.ensureNativeLibIsLoaded();
    }

    /**
     * H5Topen opens a named datatype at the location specified by loc_id and return an identifier
     * for the datatype.
     * 
     * @param loc_id A file, group, or datatype identifier.
     * @param name A datatype name.
     * @param access_plist_id Datatype access property list identifier.
     * @return a named datatype identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Topen(int loc_id, String name, int access_plist_id)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Topen(loc_id, name, access_plist_id);
        }
    }

    /**
     * H5Tcommit commits a transient datatype (not immutable) to a file, turned it into a named
     * datatype.
     * 
     * @param loc_id A file or group identifier.
     * @param name A datatype name.
     * @param type_id A datatype identifier.
     * @param link_create_plist_id Link creation property list.
     * @param dtype_create_plist_id Datatype creation property list.
     * @param dtype_access_plist_id Datatype access property list.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Tcommit(int loc_id, String name, int type_id, int link_create_plist_id,
            int dtype_create_plist_id, int dtype_access_plist_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tcommit(loc_id, name, type_id, link_create_plist_id, dtype_create_plist_id,
                    dtype_access_plist_id);
        }
    }

    /**
     * H5Tcommitted queries a type to determine whether the type specified by the type identifier is
     * a named type or a transient type.
     * 
     * @param type Datatype identifier.
     * @return true if successfully committed
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static boolean H5Tcommitted(int type) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tcommitted(type);
        }
    }

    /**
     * H5Tcreate creates a new dataype of the specified class with the specified number of bytes.
     * 
     * @param dclass Class of datatype to create.
     * @param size The number of bytes in the datatype to create.
     * @return datatype identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tcreate(int dclass, int size) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tcreate(dclass, size);
        }
    }

    /**
     * H5Tcopy copies an existing datatype. The returned type is always transient and unlocked.
     * 
     * @param type_id Identifier of datatype to copy. Can be a datatype identifier, a predefined
     *            datatype (defined in H5Tpublic.h), or a dataset Identifier.
     * @return a datatype identifier if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tcopy(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tcopy(type_id);
        }
    }

    /**
     * H5Tequal determines whether two datatype identifiers refer to the same datatype.
     * 
     * @param type_id1 Identifier of datatype to compare.
     * @param type_id2 Identifier of datatype to compare.
     * @return true if the datatype identifiers refer to the same datatype, else FALSE.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static boolean H5Tequal(int type_id1, int type_id2) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tequal(type_id1, type_id2);
        }
    }

    /**
     * H5Tlock locks the datatype specified by the type_id identifier, making it read-only and
     * non-destructible.
     * 
     * @param type_id Identifier of datatype to lock.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tlock(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tlock(type_id);
        }
    }

    /**
     * H5Tget_class returns the datatype class identifier.
     * 
     * @param type_id Identifier of datatype to query.
     * @return datatype class identifier if successful; otherwise H5T_NO_CLASS (-1).
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_class(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_class(type_id);
        }
    }

    /**
     * H5Tget_size returns the size of a datatype in bytes as an int value.
     * 
     * @param type_id Identifier of datatype to query.
     * @return the size of the datatype in bytes if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library, or if the size of the data
     *                type exceeds an int
     */
    public static int H5Tget_size(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_size(type_id);
        }
    }

    /**
     * H5Tget_size returns the size of a datatype in bytes as a long value.
     * 
     * @param type_id Identifier of datatype to query.
     * @return the size of the datatype in bytes if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static long H5Tget_size_long(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_size_long(type_id);
        }
    }

    /**
     * H5Tset_size sets the total size in bytes, size, for an atomic datatype (this operation is not
     * permitted on compound datatypes).
     * 
     * @param type_id Identifier of datatype to change size.
     * @param size Size in bytes to modify datatype.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_size(int type_id, int size) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_size(type_id, size);
        }
    }

    /**
     * H5Tget_order returns the byte order of an atomic datatype.
     * 
     * @param type_id Identifier of datatype to query.
     * @return a byte order constant if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_order(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_order(type_id);
        }
    }

    /**
     * H5Tset_order sets the byte ordering of an atomic datatype.
     * 
     * @param type_id Identifier of datatype to set.
     * @param order Byte ordering constant.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_order(int type_id, int order) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_order(type_id, order);
        }
    }

    /**
     * H5Tget_precision returns the precision of an atomic datatype.
     * 
     * @param type_id Identifier of datatype to query.
     * @return the number of significant bits if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_precision(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_precision(type_id);
        }
    }

    /**
     * H5Tset_precision sets the precision of an atomic datatype.
     * 
     * @param type_id Identifier of datatype to set.
     * @param precision Number of bits of precision for datatype.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_precision(int type_id, int precision) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_precision(type_id, precision);
        }
    }

    /**
     * H5Tget_offset retrieves the bit offset of the first significant bit.
     * 
     * @param type_id Identifier of datatype to query.
     * @return a positive offset value if successful; otherwise 0.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_offset(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_offset(type_id);
        }
    }

    /**
     * H5Tset_offset sets the bit offset of the first significant bit.
     * 
     * @param type_id Identifier of datatype to set.
     * @param offset Offset of first significant bit.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_offset(int type_id, int offset) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_offset(type_id, offset);
        }
    }

    /**
     * H5Tget_pad retrieves the padding type of the least and most-significant bit padding.
     * 
     * @param type_id IN: Identifier of datatype to query.
     * @param pad OUT: locations to return least-significant and most-significant bit padding type.
     * 
     *            <pre>
     * 
     *            pad[0] = lsb // least-significant bit padding type pad[1] = msb //
     *            most-significant bit padding type
     * 
     * </pre>
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - pad is null.
     */
    public static int H5Tget_pad(int type_id, int[] pad) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_pad(type_id, pad);
        }
    }

    /**
     * H5Tset_pad sets the least and most-significant bits padding types.
     * 
     * @param type_id Identifier of datatype to set.
     * @param lsb Padding type for least-significant bits.
     * @param msb Padding type for most-significant bits.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_pad(int type_id, int lsb, int msb) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_pad(type_id, lsb, msb);
        }
    }

    /**
     * H5Tget_sign retrieves the sign type for an integer type.
     * 
     * @param type_id Identifier of datatype to query.
     * @return a valid sign type if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_sign(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_sign(type_id);
        }
    }

    /**
     * H5Tset_sign sets the sign proprety for an integer type.
     * 
     * @param type_id Identifier of datatype to set.
     * @param sign Sign type.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_sign(int type_id, int sign) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_sign(type_id, sign);
        }
    }

    /**
     * H5Tget_fields retrieves information about the locations of the various bit fields of a
     * floating point datatype.
     * 
     * @param type_id IN: Identifier of datatype to query.
     * @param fields OUT: location of size and bit-position.
     * 
     *            <pre>
     * 
     *            fields[0] = spos OUT: location to return size of in bits. fields[1] = epos OUT:
     *            location to return exponent bit-position. fields[2] = esize OUT: location to
     *            return size of exponent in bits. fields[3] = mpos OUT: location to return mantissa
     *            bit-position. fields[4] = msize OUT: location to return size of mantissa in bits.
     * 
     * </pre>
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - fileds is null.
     * @exception IllegalArgumentException - fileds array is invalid.
     */
    public static int H5Tget_fields(int type_id, int[] fields) throws HDF5LibraryException,
            NullPointerException, IllegalArgumentException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_fields(type_id, fields);
        }
    }

    /**
     * H5Tset_fields sets the locations and sizes of the various floating point bit fields.
     * 
     * @param type_id Identifier of datatype to set.
     * @param spos Size position.
     * @param epos Exponent bit position.
     * @param esize Size of exponent in bits.
     * @param mpos Mantissa bit position.
     * @param msize Size of mantissa in bits.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_fields(int type_id, int spos, int epos, int esize, int mpos, int msize)
            throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_fields(type_id, spos, epos, esize, mpos, msize);
        }
    }

    /**
     * H5Tget_ebias retrieves the exponent bias of a floating-point type.
     * 
     * @param type_id Identifier of datatype to query.
     * @return the bias if successful; otherwise 0.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_ebias(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_ebias(type_id);
        }
    }

    /**
     * H5Tset_ebias sets the exponent bias of a floating-point type.
     * 
     * @param type_id Identifier of datatype to set.
     * @param ebias Exponent bias value.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_ebias(int type_id, int ebias) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_ebias(type_id, ebias);
        }
    }

    /**
     * H5Tget_norm retrieves the mantissa normalization of a floating-point datatype.
     * 
     * @param type_id Identifier of datatype to query.
     * @return a valid normalization type if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_norm(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_norm(type_id);
        }
    }

    /**
     * H5Tset_norm sets the mantissa normalization of a floating-point datatype.
     * 
     * @param type_id Identifier of datatype to set.
     * @param norm Mantissa normalization type.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_norm(int type_id, int norm) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_norm(type_id, norm);
        }
    }

    /**
     * H5Tget_inpad retrieves the internal padding type for unused bits in floating-point datatypes.
     * 
     * @param type_id Identifier of datatype to query.
     * @return a valid padding type if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_inpad(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_inpad(type_id);
        }
    }

    /**
     * If any internal bits of a floating point type are unused (that is, those significant bits
     * which are not part of the sign, exponent, or mantissa), then H5Tset_inpad will be filled
     * according to the value of the padding value property inpad.
     * 
     * @param type_id Identifier of datatype to modify.
     * @param inpad Padding type.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_inpad(int type_id, int inpad) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_inpad(type_id, inpad);
        }
    }

    /**
     * H5Tget_cset retrieves the character set type of a string datatype.
     * 
     * @param type_id Identifier of datatype to query.
     * @return a valid character set type if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_cset(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_cset(type_id);
        }
    }

    /**
     * H5Tset_cset the character set to be used.
     * 
     * @param type_id Identifier of datatype to modify.
     * @param cset Character set type.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_cset(int type_id, int cset) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_cset(type_id, cset);
        }
    }

    /**
     * H5Tget_strpad retrieves the string padding method for a string datatype.
     * 
     * @param type_id Identifier of datatype to query.
     * @return a valid string padding type if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_strpad(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_strpad(type_id);
        }
    }

    /**
     * H5Tset_strpad defines the storage mechanism for the string.
     * 
     * @param type_id Identifier of datatype to modify.
     * @param strpad String padding type.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_strpad(int type_id, int strpad) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_strpad(type_id, strpad);
        }
    }

    /**
     * H5Tget_nmembers retrieves the number of fields a compound datatype has.
     * 
     * @param type_id Identifier of datatype to query.
     * @return number of members datatype has if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_nmembers(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_nmembers(type_id);
        }
    }

    /**
     * H5Tget_member_name retrieves the name of a field of a compound datatype.
     * 
     * @param type_id Identifier of datatype to query.
     * @param field_idx Field index (0-based) of the field name to retrieve.
     * @return a valid pointer if successful; otherwise null.
     */
    public static String H5Tget_member_name(int type_id, int field_idx)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_member_name(type_id, field_idx);
        }
    }

    /**
     * H5Tget_member_index retrieves the index of a field of a compound datatype.
     * 
     * @param type_id Identifier of datatype to query.
     * @param field_name Field name of the field index to retrieve.
     * @return if field is defined, the index; else negative.
     */
    public static int H5Tget_member_index(int type_id, String field_name)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_member_index(type_id, field_name);
        }
    }

    /**
     * H5Tget_member_class returns the datatype of the specified member.
     * 
     * @param type_id Identifier of datatype to query.
     * @param field_idx Field index (0-based) of the field type to retrieve.
     * @return the identifier of a copy of the datatype of the field if successful;
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_member_class(int type_id, int field_idx) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_member_class(type_id, field_idx);
        }
    }

    /**
     * H5Tget_member_type returns the datatype of the specified member.
     * 
     * @param type_id Identifier of datatype to query.
     * @param field_idx Field index (0-based) of the field type to retrieve.
     * @return the identifier of a copy of the datatype of the field if successful;
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_member_type(int type_id, int field_idx) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_member_type(type_id, field_idx);
        }
    }

    /**
     * H5Tget_member_offset returns the byte offset of the specified member of the compound
     * datatype. This is the byte offset in the HDF-5 file/library, NOT the offset of any Java
     * object which might be mapped to this data item.
     * 
     * @param type_id Identifier of datatype to query.
     * @param membno Field index (0-based) of the field type to retrieve.
     * @return the offset of the member.
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static long H5Tget_member_offset(int type_id, int membno) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_member_offset(type_id, membno);
        }
    }

    /**
     * H5Tinsert adds another member to the compound datatype type_id.
     * 
     * @param type_id Identifier of compound datatype to modify.
     * @param name Name of the field to insert.
     * @param offset Offset in memory structure of the field to insert.
     * @param field_id Datatype identifier of the field to insert.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Tinsert(int type_id, String name, long offset, int field_id)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tinsert(type_id, name, offset, field_id);
        }
    }

    /**
     * H5Tpack recursively removes padding from within a compound datatype to make it more efficient
     * (space-wise) to store that data.
     * <P>
     * <b>WARNING:</b> This call only affects the C-data, even if it succeeds, there may be no
     * visible effect on Java objects.
     * 
     * @param type_id Identifier of datatype to modify.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tpack(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tpack(type_id);
        }
    }

    /**
     * H5Tclose releases a datatype.
     * 
     * @param type_id Identifier of datatype to release.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tclose(int type_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tclose(type_id);
        }
    }

    /**
     * H5Tenum_create creates a new enumeration datatype based on the specified base datatype,
     * parent_id, which must be an integer type.
     * 
     * @param base_id Identifier of the parent datatype to release.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tenum_create(int base_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tenum_create(base_id);
        }
    }

    /**
     * H5Tenum_insert inserts a new enumeration datatype member into an 8bit enumeration datatype.
     * 
     * @param type Identifier of datatype.
     * @param name The name of the member
     * @param value The value of the member, data of the correct type
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Tenum_insert(int type, String name, byte value)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tenum_insert(type, name, value);
        }
    }

    /**
     * H5Tenum_insert inserts a new enumeration datatype member into a 16bit enumeration datatype.
     * 
     * @param type Identifier of datatype.
     * @param name The name of the member
     * @param value The value of the member, data of the correct type
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Tenum_insert(int type, String name, short value)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tenum_insert(type, name, value);
        }
    }

    /**
     * H5Tenum_insert inserts a new enumeration datatype member into a 32bit enumeration datatype.
     * 
     * @param type Identifier of datatype.
     * @param name The name of the member
     * @param value The value of the member, data of the correct type
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Tenum_insert(int type, String name, int value) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tenum_insert(type, name, value);
        }
    }

    /**
     * Converts the <var>value</var> (in place) to little endian.
     * 
     * @return a non-negative value if successful
     */
    public static int H5Tconvert_to_little_endian(short[] value)

    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tconvert_to_little_endian(value);
        }
    }

    /**
     * Converts the <var>value</var> (in place) to little endian.
     * 
     * @return a non-negative value if successful
     */
    public static int H5Tconvert_to_little_endian(int[] value)
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tconvert_to_little_endian(value);
        }
    }

    /**
     * H5Tenum_nameof finds the symbol name that corresponds to the specified value of the
     * enumeration datatype type.
     * 
     * @param type IN: Identifier of datatype.
     * @param value IN: The value of the member, data of the correct
     * @param name OUT: The name of the member
     * @param size IN: The max length of the name
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Tenum_nameof(int type, int[] value, String[] name, int size)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tenum_nameof(type, value, name, size);
        }
    }

    /**
     * H5Tenum_valueof finds the value that corresponds to the specified name of the enumeration
     * datatype type.
     * 
     * @param type IN: Identifier of datatype.
     * @param name IN: The name of the member
     * @param value OUT: The value of the member
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Tenum_valueof(int type, String name, int[] value)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tenum_valueof(type, name, value);
        }
    }

    /**
     * H5Tvlen_create creates a new variable-length (VL) dataype.
     * 
     * @param base_id IN: Identifier of parent datatype.
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tvlen_create(int base_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tvlen_create(base_id);
        }
    }

    /**
     * H5Tset_tag tags an opaque datatype type_id with a unique ASCII identifier tag.
     * 
     * @param type IN: Identifier of parent datatype.
     * @param tag IN: Name of the tag (will be stored as ASCII)
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tset_tag(int type, String tag) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tset_tag(type, tag);
        }
    }

    /**
     * H5Tget_tag returns the tag associated with datatype type_id.
     * 
     * @param type IN: Identifier of datatype.
     * @return the tag
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static String H5Tget_tag(int type) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_tag(type);
        }
    }

    /**
     * H5Tget_super returns the type from which TYPE is derived.
     * 
     * @param type IN: Identifier of datatype.
     * @return the parent type
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     */
    public static int H5Tget_super(int type) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_super(type);
        }
    }

    /**
     * H5Tget_member_value returns the value of the enumeration datatype member memb_no.
     * 
     * @param type_id IN: Identifier of datatype.
     * @param membno IN: The name of the member
     * @param value OUT: The value of the member
     * @return a non-negative value if successful
     * @exception HDF5LibraryException - Error from the HDF-5 Library.
     * @exception NullPointerException - name is null.
     */
    public static int H5Tget_member_value(int type_id, int membno, int[] value)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_member_value(type_id, membno, value);
        }
    }

    /**
     * Creates an array datatype object.
     * 
     * @param base_type_id Datatype identifier for the array base datatype.
     * @param rank Rank of the array.
     * @param dims Size of each array dimension.
     * @return a valid datatype identifier if successful; otherwise returns a negative value.
     * @exception HDF5LibraryException Error from the HDF5 Library.
     * @exception NullPointerException rank is &lt; 1 or dims is null.
     */
    public static int H5Tarray_create(int base_type_id, int rank, int[] dims)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tarray_create(base_type_id, rank, dims);
        }
    }

    /**
     * Returns the rank of an array datatype.
     * 
     * @param adtype_id Datatype identifier of array object.
     * @return the rank of the array if successful; otherwise returns a negative value.
     * @exception HDF5LibraryException Error from the HDF5 Library.
     */
    public static int H5Tget_array_ndims(int adtype_id) throws HDF5LibraryException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_array_ndims(adtype_id);
        }
    }

    /**
     * Returns sizes of array dimensions.
     * 
     * @param adtype_id IN: Datatype identifier of array object.
     * @param dims OUT: Sizes of array dimensions.
     * @return the non-negative number of dimensions of the array type if successful; otherwise
     *         returns a negative value.
     * @exception HDF5LibraryException Error from the HDF5 Library.
     * @exception NullPointerException dims is null.
     */
    public static int H5Tget_array_dims(int adtype_id, int[] dims) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_array_dims(adtype_id, dims);
        }
    }

    public static int H5Tget_native_type(int tid, int alloc_time) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_native_type(tid, alloc_time);
        }
    }

    public static int H5Tget_native_type(final int tid) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tget_native_type(tid);
        }
    }

    public static boolean H5Tis_variable_str(int dtype_id) throws HDF5LibraryException,
            NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tis_variable_str(dtype_id);
        }
    }

    public static boolean H5Tdetect_class(int dtype_id, int dtype_class)
            throws HDF5LibraryException, NullPointerException
    {
        synchronized (ncsa.hdf.hdf5lib.H5.class)
        {
            return H5.H5Tdetect_class(dtype_id, dtype_class);
        }
    }

}
