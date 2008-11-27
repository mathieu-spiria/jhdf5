/****************************************************************************
 * NCSA HDF                                                                 *
 * National Comptational Science Alliance                                   *
 * University of Illinois at Urbana-Champaign                               *
 * 605 E. Springfield, Champaign IL 61820                                   *
 *                                                                          *
 * For conditions of distribution and use, see the accompanying             *
 * hdf-java/COPYING file.                                                   *
 *                                                                          *
 ****************************************************************************/

package ncsa.hdf.hdf5lib;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * This is a class for handling multidimensional arrays for HDF.
 * <p>
 * The purpose is to allow the storage and retrieval of arbitrary array types containing scientific
 * data.
 * <p>
 * The methods support the conversion of an array to and from Java to a one-dimensional array of
 * bytes suitable for I/O by the C library.
 * <p>
 * This class heavily uses the <a href="./ncsa.hdf.hdf5lib.HDFNativeData.html">HDFNativeData</a>
 * class to convert between Java and C representations.
 */

public class HDFArray
{

    private Object _theArray = null;

    private ArrayDescriptor _desc = null;

    private byte[] _barray = null;

    // public HDFArray() {}

    /**
     * The input must be a Java Array (possibly multidimensional) of primitive numbers or
     * sub-classes of Number.
     * <P>
     * The input is analysed to determine the number of dimensions and size of each dimension, as
     * well as the type of the elements.
     * <P>
     * The description is saved in private variables, and used to convert data.
     * 
     * @exception ncsa.hdf.hdf5lib.exceptions.HDF5Exception object is not an array.
     */
    public HDFArray(final Object anArray) throws HDF5Exception
    {

        if (anArray == null)
        {
            throw new HDF5JavaException("HDFArray: array is null?: ");
        }
        final Class<?> tc = anArray.getClass();
        if (tc.isArray() == false)
        {
            /* exception: not an array */
            final HDF5JavaException ex = new HDF5JavaException("HDFArray: not an array?: ");
            throw (ex);
        }
        _theArray = anArray;
        _desc = new ArrayDescriptor(_theArray);

        /* extra error checking -- probably not needed */
        if (_desc == null)
        {
            final HDF5JavaException ex =
                    new HDF5JavaException("HDFArray: internal error: array description failed?: ");
            throw (ex);
        }
    }

    /**
     * The input must be a Java Array (possibly multidimensional) of primitive numbers or
     * sub-classes of Number.
     * <P>
     * The input is analysed to determine the number of dimensions and size of each dimension, as
     * well as the type of the elements.
     * <P>
     * The description is saved in private variables, and used to convert data.
     * 
     * @exception ncsa.hdf.hdf5lib.exceptions.HDF5Exception object is not an array.
     */
    public HDFArray(final String[] anArray, final int maxLength) throws HDF5Exception
    {

        if (anArray == null)
        {
            throw new HDF5JavaException("HDFArray: array is null?: ");
        }
        _theArray = anArray;
        _desc = new ArrayDescriptor(anArray, maxLength);

        /* extra error checking -- probably not needed */
        if (_desc == null)
        {
            final HDF5JavaException ex =
                    new HDF5JavaException("HDFArray: internal error: array description failed?: ");
            throw (ex);
        }
    }

    /**
     * Allocate a one-dimensional array of bytes sufficient to store the array.
     * 
     * @return A one-D array of bytes, filled with zeroes. The bytes are sufficient to hold the data
     *         of the Array passed to the constructor.
     * @exception ncsa.hdf.hdf5lib.exceptions.HDF5JavaException Allocation failed.
     */

    public byte[] emptyBytes() throws HDF5Exception
    {
        byte[] b = null;

        if ((ArrayDescriptor.dims == 1) && (ArrayDescriptor.NT == 'B'))
        {
            b = (byte[]) _theArray;
        } else
        {
            b = new byte[ArrayDescriptor.totalSize];
        }
        if (b == null)
        {
            final HDF5JavaException ex =
                    new HDF5JavaException("HDFArray: emptyBytes: allocation failed");
            throw (ex);
        }
        return (b);
    }

    /**
     * Given a Java array of numbers, convert it to a one-dimensional array of bytes in correct
     * native order.
     * 
     * @return A one-D array of bytes, constructed from the Array passed to the constructor.
     * @exception ncsa.hdf.hdf5lib.exceptions.HDF5Exception thrown for errors in HDF5
     * @exception ncsa.hdf.hdf5lib.exceptions.HDF5JavaException the object not an array or other
     *                internal error.
     */
    public byte[] byteify() throws HDF5Exception
    {

        if (_barray != null)
        {
            return _barray;
        }

        if (_theArray == null)
        {
            /* exception: not an array */
            final HDF5JavaException ex = new HDF5JavaException("HDFArray: byteify not an array?: ");
            throw (ex);
        }

        if (ArrayDescriptor.dims == 1)
        {
            /* special case */
            if (ArrayDescriptor.NT == 'B')
            {
                /* really special case! */
                _barray = (byte[]) _theArray;
                return _barray;
            } else
            {
                try
                {
                    _barray = new byte[ArrayDescriptor.totalSize];

                    byte[] therow;
                    if (ArrayDescriptor.NT == 'I')
                    {
                        therow =
                                HDFNativeData.intToByte(0, ArrayDescriptor.dimlen[1],
                                        (int[]) _theArray);
                    } else if (ArrayDescriptor.NT == 'S')
                    {
                        therow =
                                HDFNativeData.shortToByte(0, ArrayDescriptor.dimlen[1],
                                        (short[]) _theArray);
                    } else if (ArrayDescriptor.NT == 'F')
                    {
                        therow =
                                HDFNativeData.floatToByte(0, ArrayDescriptor.dimlen[1],
                                        (float[]) _theArray);
                    } else if (ArrayDescriptor.NT == 'J')
                    {
                        therow =
                                HDFNativeData.longToByte(0, ArrayDescriptor.dimlen[1],
                                        (long[]) _theArray);
                    } else if (ArrayDescriptor.NT == 'D')
                    {
                        therow =
                                HDFNativeData.doubleToByte(0, ArrayDescriptor.dimlen[1],
                                        (double[]) _theArray);
                    } else if (ArrayDescriptor.NT == 'L')
                    {
                        if (ArrayDescriptor.className.equals("java.lang.Byte"))
                        {
                            therow = ByteObjToByte((Byte[]) _theArray);
                        } else if (ArrayDescriptor.className.equals("java.lang.Integer"))
                        {
                            therow = IntegerToByte((Integer[]) _theArray);
                        } else if (ArrayDescriptor.className.equals("java.lang.Short"))
                        {
                            therow = ShortToByte((Short[]) _theArray);
                        } else if (ArrayDescriptor.className.equals("java.lang.Float"))
                        {
                            therow = FloatObjToByte((Float[]) _theArray);
                        } else if (ArrayDescriptor.className.equals("java.lang.Double"))
                        {
                            therow = DoubleObjToByte((Double[]) _theArray);
                        } else if (ArrayDescriptor.className.equals("java.lang.Long"))
                        {
                            therow = LongObjToByte((Long[]) _theArray);
                        } else if (ArrayDescriptor.className.equals("java.lang.String"))
                        {
                            therow = StringObjToByte((String[]) _theArray, ArrayDescriptor.NTsize);
                        } else
                        {
                            final HDF5JavaException ex =
                                    new HDF5JavaException("HDFArray: unknown type of Object?");
                            throw (ex);
                        }
                    } else
                    {
                        final HDF5JavaException ex =
                                new HDF5JavaException("HDFArray: unknown type of data?");
                        throw (ex);
                    }
                    System.arraycopy(therow, 0, _barray, 0,
                            (ArrayDescriptor.dimlen[1] * ArrayDescriptor.NTsize));
                    return _barray;
                } catch (final OutOfMemoryError err)
                {
                    final HDF5JavaException ex =
                            new HDF5JavaException("HDFArray: byteify array too big?");
                    throw (ex);
                }
            }
        }

        try
        {
            _barray = new byte[ArrayDescriptor.totalSize];
        } catch (final OutOfMemoryError err)
        {
            final HDF5JavaException ex = new HDF5JavaException("HDFArray: byteify array too big?");
            throw (ex);
        }

        Object oo = _theArray;
        int n = 0; /* the current byte */
        int index = 0;
        int i;
        while (n < ArrayDescriptor.totalSize)
        {
            oo = ArrayDescriptor.objs[0];
            index = n / ArrayDescriptor.bytetoindex[0];
            index %= ArrayDescriptor.dimlen[0];
            for (i = 0; i < (ArrayDescriptor.dims); i++)
            {
                index = n / ArrayDescriptor.bytetoindex[i];
                index %= ArrayDescriptor.dimlen[i];

                if (index == ArrayDescriptor.currentindex[i])
                {
                    /* then use cached copy */
                    oo = ArrayDescriptor.objs[i];
                } else
                {
                    /* check range of index */
                    if (index > (ArrayDescriptor.dimlen[i] - 1))
                    {
                        throw new java.lang.IndexOutOfBoundsException(
                                "HDFArray: byteify index OOB?");
                    }
                    oo = java.lang.reflect.Array.get(oo, index);
                    ArrayDescriptor.currentindex[i] = index;
                    ArrayDescriptor.objs[i] = oo;
                }
            }

            /* byte-ify */
            byte arow[];
            try
            {
                if (ArrayDescriptor.NT == 'J')
                {
                    arow =
                            HDFNativeData.longToByte(0,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                    (long[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                    arow =
                            HDFNativeData.longToByte(0,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                    (long[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                } else if (ArrayDescriptor.NT == 'I')
                {
                    arow =
                            HDFNativeData.intToByte(0,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                    (int[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                } else if (ArrayDescriptor.NT == 'S')
                {
                    arow =
                            HDFNativeData.shortToByte(0,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                    (short[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                } else if (ArrayDescriptor.NT == 'B')
                {
                    arow = (byte[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1];
                } else if (ArrayDescriptor.NT == 'F')
                {
                    /* 32 bit float */
                    arow =
                            HDFNativeData.floatToByte(0,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                    (float[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                } else if (ArrayDescriptor.NT == 'D')
                {
                    /* 64 bit float */
                    arow =
                            HDFNativeData.doubleToByte(0,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                    (double[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                } else if (ArrayDescriptor.NT == 'L')
                {
                    if (ArrayDescriptor.className.equals("java.lang.Byte"))
                    {
                        arow =
                                ByteObjToByte((Byte[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                    } else if (ArrayDescriptor.className.equals("java.lang.Integer"))
                    {
                        arow =
                                IntegerToByte((Integer[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                    } else if (ArrayDescriptor.className.equals("java.lang.Short"))
                    {
                        arow =
                                ShortToByte((Short[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                    } else if (ArrayDescriptor.className.equals("java.lang.Float"))
                    {
                        arow =
                                FloatObjToByte((Float[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                    } else if (ArrayDescriptor.className.equals("java.lang.Double"))
                    {
                        arow =
                                DoubleObjToByte((Double[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                    } else if (ArrayDescriptor.className.equals("java.lang.Long"))
                    {
                        arow =
                                LongObjToByte((Long[]) ArrayDescriptor.objs[ArrayDescriptor.dims - 1]);
                    } else
                    {
                        final HDF5JavaException ex =
                                new HDF5JavaException(
                                        "HDFArray: byteify Object type not implemented?");
                        throw (ex);
                    }
                } else
                {
                    final HDF5JavaException ex =
                            new HDF5JavaException("HDFArray: byteify unknown type not implemented?");
                    throw (ex);
                }
                System.arraycopy(arow, 0, _barray, n,
                        (ArrayDescriptor.dimlen[ArrayDescriptor.dims] * ArrayDescriptor.NTsize));
                n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
            } catch (final OutOfMemoryError err)
            {
                final HDF5JavaException ex =
                        new HDF5JavaException("HDFArray: byteify array too big?");
                throw (ex);
            }
        }
        /* assert: the whole array is completed--currentindex should == len - 1 */

        /* error checks */

        if (n < ArrayDescriptor.totalSize)
        {
            throw new java.lang.InternalError(new String(
                    "HDFArray::byteify: Panic didn't complete all input data: n=  " + n
                            + " size = " + ArrayDescriptor.totalSize));
        }
        for (i = 0; i < ArrayDescriptor.dims; i++)
        {
            if (ArrayDescriptor.currentindex[i] != ArrayDescriptor.dimlen[i] - 1
                    && ArrayDescriptor.totalSize > 0)
            {
                throw new java.lang.InternalError(new String(
                        "Panic didn't complete all data: currentindex[" + i + "] = "
                                + ArrayDescriptor.currentindex[i] + " (should be "
                                + (ArrayDescriptor.dimlen[i] - 1) + " ?)"));
            }
        }
        return _barray;
    }

    /**
     * Given a one-dimensional array of bytes representing numbers, convert it to a java array of
     * the shape and size passed to the constructor.
     * 
     * @param bytes The bytes to construct the Array.
     * @return An Array (possibly multidimensional) of primitive or number objects.
     * @exception ncsa.hdf.hdf5lib.exceptions.HDF5Exception thrown for errors in HDF5
     * @exception ncsa.hdf.hdf5lib.exceptions.HDF5JavaException the object not an array or other
     *                internal error.
     */
    public Object arrayify(final byte[] bytes) throws HDF5Exception
    {

        if (_theArray == null)
        {
            /* exception: not an array */
            final HDF5JavaException ex = new HDF5JavaException("arrayify: not an array?: ");
            throw (ex);
        }

        if (java.lang.reflect.Array.getLength(bytes) != ArrayDescriptor.totalSize)
        {
            /* exception: array not right size */
            final HDF5JavaException ex = new HDF5JavaException("arrayify: array is wrong size?: ");
            throw (ex);
        }
        _barray = bytes; /* hope that the bytes are correct.... */
        if (ArrayDescriptor.dims == 1)
        {
            /* special case */
            /* 2 data copies here! */
            try
            {
                if (ArrayDescriptor.NT == 'I')
                {
                    final int[] x = HDFNativeData.byteToInt(_barray);
                    System.arraycopy(x, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                    return _theArray;
                } else if (ArrayDescriptor.NT == 'S')
                {
                    final short[] x = HDFNativeData.byteToShort(_barray);
                    System.arraycopy(x, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                    return _theArray;
                } else if (ArrayDescriptor.NT == 'F')
                {
                    final float x[] = HDFNativeData.byteToFloat(_barray);
                    System.arraycopy(x, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                    return _theArray;
                } else if (ArrayDescriptor.NT == 'J')
                {
                    final long x[] = HDFNativeData.byteToLong(_barray);
                    System.arraycopy(x, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                    return _theArray;
                } else if (ArrayDescriptor.NT == 'D')
                {
                    final double x[] = HDFNativeData.byteToDouble(_barray);
                    System.arraycopy(x, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                    return _theArray;
                } else if (ArrayDescriptor.NT == 'B')
                {
                    System.arraycopy(_barray, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                    return _theArray;
                } else if (ArrayDescriptor.NT == 'L')
                {
                    if (ArrayDescriptor.className.equals("java.lang.Byte"))
                    {
                        final Byte I[] = ByteToByteObj(_barray);
                        System.arraycopy(I, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                        return _theArray;
                    } else if (ArrayDescriptor.className.equals("java.lang.Integer"))
                    {
                        final Integer I[] = ByteToInteger(_barray);
                        System.arraycopy(I, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                        return _theArray;
                    } else if (ArrayDescriptor.className.equals("java.lang.Short"))
                    {
                        final Short I[] = ByteToShort(_barray);
                        System.arraycopy(I, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                        return _theArray;
                    } else if (ArrayDescriptor.className.equals("java.lang.Float"))
                    {
                        final Float I[] = ByteToFloatObj(_barray);
                        System.arraycopy(I, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                        return _theArray;
                    } else if (ArrayDescriptor.className.equals("java.lang.Double"))
                    {
                        final Double I[] = ByteToDoubleObj(_barray);
                        System.arraycopy(I, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                        return _theArray;
                    } else if (ArrayDescriptor.className.equals("java.lang.Long"))
                    {
                        final Long I[] = ByteToLongObj(_barray);
                        System.arraycopy(I, 0, _theArray, 0, ArrayDescriptor.dimlen[1]);
                        return _theArray;
                    } else
                    {
                        final HDF5JavaException ex =
                                new HDF5JavaException(
                                        "arrayify:  Object type not implemented yet...");
                        throw (ex);
                    }
                } else
                {
                    final HDF5JavaException ex =
                            new HDF5JavaException("arrayify:  unknown type not implemented yet...");
                    throw (ex);
                }
            } catch (final OutOfMemoryError err)
            {
                final HDF5JavaException ex =
                        new HDF5JavaException("HDFArray: arrayify array too big?");
                throw (ex);
            }
        }
        /* Assert dims >= 2 */

        Object oo = _theArray;
        int n = 0; /* the current byte */
        int index = 0;
        int i;
        while (n < ArrayDescriptor.totalSize)
        {
            oo = ArrayDescriptor.objs[0];
            index = n / ArrayDescriptor.bytetoindex[0];
            index %= ArrayDescriptor.dimlen[0];
            for (i = 0; i < (ArrayDescriptor.dims); i++)
            {
                index = n / ArrayDescriptor.bytetoindex[i];
                index %= ArrayDescriptor.dimlen[i];

                if (index == ArrayDescriptor.currentindex[i])
                {
                    /* then use cached copy */
                    oo = ArrayDescriptor.objs[i];
                } else
                {
                    /* check range of index */
                    if (index > (ArrayDescriptor.dimlen[i] - 1))
                    {
                        System.out.println("out of bounds?");
                        return null;
                    }
                    oo = java.lang.reflect.Array.get(oo, index);
                    ArrayDescriptor.currentindex[i] = index;
                    ArrayDescriptor.objs[i] = oo;
                }
            }

            /* array-ify */
            try
            {
                if (ArrayDescriptor.NT == 'J')
                {
                    final long[] arow =
                            HDFNativeData.byteToLong(n,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims], _barray);
                    java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                            (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), arow);
                    n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                    ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                } else if (ArrayDescriptor.NT == 'I')
                {
                    final int[] arow =
                            HDFNativeData.byteToInt(n,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims], _barray);
                    java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                            (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), arow);

                    n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                    ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                } else if (ArrayDescriptor.NT == 'S')
                {
                    final short[] arow =
                            HDFNativeData.byteToShort(n,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims], _barray);
                    java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                            (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), arow);

                    n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                    ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                } else if (ArrayDescriptor.NT == 'B')
                {
                    System.arraycopy(_barray, n, ArrayDescriptor.objs[ArrayDescriptor.dims - 1], 0,
                            ArrayDescriptor.dimlen[ArrayDescriptor.dims]);
                    n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                } else if (ArrayDescriptor.NT == 'F')
                {
                    final float arow[] =
                            HDFNativeData.byteToFloat(n,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims], _barray);
                    java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                            (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), arow);

                    n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                    ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                } else if (ArrayDescriptor.NT == 'D')
                {
                    final double[] arow =
                            HDFNativeData.byteToDouble(n,
                                    ArrayDescriptor.dimlen[ArrayDescriptor.dims], _barray);
                    java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                            (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), arow);

                    n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                    ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                } else if (ArrayDescriptor.NT == 'L')
                {
                    if (ArrayDescriptor.className.equals("java.lang.Byte"))
                    {
                        final Byte I[] =
                                ByteToByteObj(n, ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                        _barray);
                        java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                                (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), I);

                        n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                        ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                    } else if (ArrayDescriptor.className.equals("java.lang.Integer"))
                    {
                        final Integer I[] =
                                ByteToInteger(n, ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                        _barray);
                        java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                                (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), I);

                        n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                        ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                    } else if (ArrayDescriptor.className.equals("java.lang.Short"))
                    {
                        final Short I[] =
                                ByteToShort(n, ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                        _barray);
                        java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                                (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), I);

                        n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                        ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                    } else if (ArrayDescriptor.className.equals("java.lang.Float"))
                    {
                        final Float I[] =
                                ByteToFloatObj(n, ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                        _barray);
                        java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                                (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), I);

                        n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                        ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                    } else if (ArrayDescriptor.className.equals("java.lang.Double"))
                    {
                        final Double I[] =
                                ByteToDoubleObj(n, ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                        _barray);
                        java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                                (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), I);

                        n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                        ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                    } else if (ArrayDescriptor.className.equals("java.lang.Long"))
                    {
                        final Long I[] =
                                ByteToLongObj(n, ArrayDescriptor.dimlen[ArrayDescriptor.dims],
                                        _barray);
                        java.lang.reflect.Array.set(ArrayDescriptor.objs[ArrayDescriptor.dims - 2],
                                (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]), I);

                        n += ArrayDescriptor.bytetoindex[ArrayDescriptor.dims - 1];
                        ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1]++;
                    } else
                    {
                        final HDF5JavaException ex =
                                new HDF5JavaException("HDFArray: unsupported Object type: "
                                        + ArrayDescriptor.NT);
                        throw (ex);
                    }
                } else
                {
                    final HDF5JavaException ex =
                            new HDF5JavaException("HDFArray: unknown or unsupported type: "
                                    + ArrayDescriptor.NT);
                    throw (ex);
                }
            } catch (final OutOfMemoryError err)
            {
                final HDF5JavaException ex =
                        new HDF5JavaException("HDFArray: arrayify array too big?");
                throw (ex);
            }

        }

        /* assert: the whole array is completed--currentindex should == len - 1 */

        /* error checks */

        if (n < ArrayDescriptor.totalSize)
        {
            throw new java.lang.InternalError(new String(
                    "HDFArray::arrayify Panic didn't complete all input data: n=  " + n
                            + " size = " + ArrayDescriptor.totalSize));
        }
        for (i = 0; i <= ArrayDescriptor.dims - 2; i++)
        {
            if (ArrayDescriptor.currentindex[i] != ArrayDescriptor.dimlen[i] - 1)
            {
                throw new java.lang.InternalError(new String(
                        "HDFArray::arrayify Panic didn't complete all data: currentindex[" + i
                                + "] = " + ArrayDescriptor.currentindex[i] + " (should be "
                                + (ArrayDescriptor.dimlen[i] - 1) + "?"));
            }
        }
        if (ArrayDescriptor.NT != 'B')
        {
            if (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1] != ArrayDescriptor.dimlen[ArrayDescriptor.dims - 1]
                    && ArrayDescriptor.totalSize > 0)
            {
                throw new java.lang.InternalError(new String(
                        "HDFArray::arrayify Panic didn't complete all data: currentindex[" + i
                                + "] = " + ArrayDescriptor.currentindex[i] + " (should be "
                                + (ArrayDescriptor.dimlen[i]) + "?"));
            }
        } else
        {
            if (ArrayDescriptor.currentindex[ArrayDescriptor.dims - 1] != (ArrayDescriptor.dimlen[ArrayDescriptor.dims - 1] - 1)
                    && ArrayDescriptor.totalSize > 0)
            {
                throw new java.lang.InternalError(new String(
                        "HDFArray::arrayify Panic didn't complete all data: currentindex[" + i
                                + "] = " + ArrayDescriptor.currentindex[i] + " (should be "
                                + (ArrayDescriptor.dimlen[i] - 1) + "?"));
            }
        }

        return _theArray;
    }

    private byte[] IntegerToByte(final Integer in[])
    {
        final int nelems = java.lang.reflect.Array.getLength(in);
        final int[] out = new int[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = in[i].intValue();
        }
        return HDFNativeData.intToByte(0, nelems, out);
    }

    private Integer[] ByteToInteger(final byte[] bin)
    {
        final int in[] = HDFNativeData.byteToInt(bin);
        final int nelems = java.lang.reflect.Array.getLength(in);
        final Integer[] out = new Integer[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Integer(in[i]);
        }
        return out;
    }

    private Integer[] ByteToInteger(final int start, final int len, final byte[] bin)
    {
        final int in[] = HDFNativeData.byteToInt(start, len, bin);
        final int nelems = java.lang.reflect.Array.getLength(in);
        final Integer[] out = new Integer[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Integer(in[i]);
        }
        return out;
    }

    private byte[] ShortToByte(final Short in[])
    {
        final int nelems = java.lang.reflect.Array.getLength(in);
        final short[] out = new short[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = in[i].shortValue();
        }
        return HDFNativeData.shortToByte(0, nelems, out);
    }

    private Short[] ByteToShort(final byte[] bin)
    {
        final short in[] = HDFNativeData.byteToShort(bin);
        final int nelems = java.lang.reflect.Array.getLength(in);
        final Short[] out = new Short[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Short(in[i]);
        }
        return out;
    }

    private Short[] ByteToShort(final int start, final int len, final byte[] bin)
    {
        final short in[] = HDFNativeData.byteToShort(start, len, bin);
        final int nelems = java.lang.reflect.Array.getLength(in);
        final Short[] out = new Short[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Short(in[i]);
        }
        return out;
    }

    private byte[] ByteObjToByte(final Byte in[])
    {
        final int nelems = java.lang.reflect.Array.getLength(in);
        final byte[] out = new byte[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = in[i].byteValue();
        }
        return out;
    }

    private Byte[] ByteToByteObj(final byte[] bin)
    {
        final int nelems = java.lang.reflect.Array.getLength(bin);
        final Byte[] out = new Byte[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Byte(bin[i]);
        }
        return out;
    }

    private Byte[] ByteToByteObj(final int start, final int len, final byte[] bin)
    {
        final Byte[] out = new Byte[len];

        for (int i = 0; i < len; i++)
        {
            out[i] = new Byte(bin[i]);
        }
        return out;
    }

    private byte[] FloatObjToByte(final Float in[])
    {
        final int nelems = java.lang.reflect.Array.getLength(in);
        final float[] out = new float[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = in[i].floatValue();
        }
        return HDFNativeData.floatToByte(0, nelems, out);
    }

    private Float[] ByteToFloatObj(final byte[] bin)
    {
        final float in[] = HDFNativeData.byteToFloat(bin);
        final int nelems = java.lang.reflect.Array.getLength(in);
        final Float[] out = new Float[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Float(in[i]);
        }
        return out;
    }

    private Float[] ByteToFloatObj(final int start, final int len, final byte[] bin)
    {
        final float in[] = HDFNativeData.byteToFloat(start, len, bin);
        final int nelems = java.lang.reflect.Array.getLength(in);
        final Float[] out = new Float[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Float(in[i]);
        }
        return out;
    }

    private byte[] DoubleObjToByte(final Double in[])
    {
        final int nelems = java.lang.reflect.Array.getLength(in);
        final double[] out = new double[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = in[i].doubleValue();
        }
        return HDFNativeData.doubleToByte(0, nelems, out);
    }

    private Double[] ByteToDoubleObj(final byte[] bin)
    {
        final double in[] = HDFNativeData.byteToDouble(bin);
        final int nelems = java.lang.reflect.Array.getLength(in);
        final Double[] out = new Double[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Double(in[i]);
        }
        return out;
    }

    private Double[] ByteToDoubleObj(final int start, final int len, final byte[] bin)
    {
        final double in[] = HDFNativeData.byteToDouble(start, len, bin);
        final int nelems = java.lang.reflect.Array.getLength(in);
        final Double[] out = new Double[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Double(in[i]);
        }
        return out;
    }

    private byte[] LongObjToByte(final Long in[])
    {
        final int nelems = java.lang.reflect.Array.getLength(in);
        final long[] out = new long[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = in[i].longValue();
        }
        return HDFNativeData.longToByte(0, nelems, out);
    }

    private Long[] ByteToLongObj(final byte[] bin)
    {
        final long in[] = HDFNativeData.byteToLong(bin);
        final int nelems = java.lang.reflect.Array.getLength(in);
        final Long[] out = new Long[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Long(in[i]);
        }
        return out;
    }

    private Long[] ByteToLongObj(final int start, final int len, final byte[] bin)
    {
        final long in[] = HDFNativeData.byteToLong(start, len, bin);
        final int nelems = java.lang.reflect.Array.getLength(in);
        final Long[] out = new Long[nelems];

        for (int i = 0; i < nelems; i++)
        {
            out[i] = new Long(in[i]);
        }
        return out;
    }

    private byte[] StringObjToByte(final String[] in, final int maxLength)
    {
        final int nelems = in.length;
        final byte[] out = new byte[nelems * maxLength];

        for (int i = 0; i < nelems; i++)
        {
            final byte[] bytes = (in[i] + '\0').getBytes();
            System.arraycopy(bytes, 0, out, i * maxLength, bytes.length);
        }
        return out;
    }

}

/**
 * This private class is used by HDFArray to discover the shape and type of an arbitrary array.
 * <p>
 * We use java.lang.reflection here.
 */
class ArrayDescriptor
{

    static String theType = "";

    static Class<?> theClass = null;

    static int[] dimlen = null;

    static int[] dimstart = null;

    static int[] currentindex = null;

    static int[] bytetoindex = null;

    static int totalSize = 0;

    static Object[] objs = null;

    static char NT = ' '; /* must be B,S,I,L,F,D, else error */

    static int NTsize = 0;

    static int dims = 0;

    static String className;

    public ArrayDescriptor(final Object anArray) throws HDF5Exception
    {

        final Class<?> tc = anArray.getClass();
        if (tc.isArray() == false)
        {
            /* exception: not an array */
            final HDF5Exception ex = new HDF5JavaException("ArrayDescriptor: not an array?: ");
            throw (ex);
        }

        theClass = tc;

        /*
         * parse the type descriptor to discover the shape of the array
         */
        final String ss = tc.toString();
        theType = ss;
        int n = 6;
        dims = 0;
        char c = ' ';
        while (n < ss.length())
        {
            c = ss.charAt(n);
            n++;
            if (c == '[')
            {
                dims++;
            }
        }

        final String css = ss.substring(ss.lastIndexOf('[') + 1);
        NT = c; /* must be B,S,I,L,F,D, else error */
        if (NT == 'B')
        {
            NTsize = 1;
        } else if (NT == 'S')
        {
            NTsize = 2;
        } else if ((NT == 'I') || (NT == 'F'))
        {
            NTsize = 4;
        } else if ((NT == 'J') || (NT == 'D'))
        {
            NTsize = 8;
        } else if (css.startsWith("Ljava.lang.Byte"))
        {
            NT = 'L';
            className = "java.lang.Byte";
            NTsize = 1;
        } else if (css.startsWith("Ljava.lang.Short"))
        {
            NT = 'L';
            className = "java.lang.Short";
            NTsize = 2;
        } else if (css.startsWith("Ljava.lang.Integer"))
        {
            NT = 'L';
            className = "java.lang.Integer";
            NTsize = 4;
        } else if (css.startsWith("Ljava.lang.Float"))
        {
            NT = 'L';
            className = "java.lang.Float";
            NTsize = 4;
        } else if (css.startsWith("Ljava.lang.Double"))
        {
            NT = 'L';
            className = "java.lang.Double";
            NTsize = 8;
        } else if (css.startsWith("Ljava.lang.Long"))
        {
            NT = 'L';
            className = "java.lang.Long";
            NTsize = 8;
        } else if (css.startsWith("Ljava.lang.String"))
        {
            throw new HDF5JavaException(new String(
                    "ArrayDesciptor: Error:  String array not supported yet"));
        } else
        {
            /* exception: not a numeric type */
            throw new HDF5JavaException(new String(
                    "ArrayDesciptor: Error:  array is not numeric (type is " + css + ") ?"));
        }

        /* fill in the table */
        dimlen = new int[dims + 1];
        dimstart = new int[dims + 1];
        currentindex = new int[dims + 1];
        bytetoindex = new int[dims + 1];
        objs = new Object[dims + 1];

        Object o = anArray;
        objs[0] = o;
        dimlen[0] = 1;
        dimstart[0] = 0;
        currentindex[0] = 0;
        int i;
        for (i = 1; i <= dims; i++)
        {
            dimlen[i] = java.lang.reflect.Array.getLength(o);
            if (dimlen[i] > 0)
            {
                o = java.lang.reflect.Array.get(o, 0);
                objs[i] = o;
            }
            dimstart[i] = 0;
            currentindex[i] = 0;
        }

        int j;
        int dd;
        bytetoindex[dims] = NTsize;
        for (i = dims; i >= 0; i--)
        {
            dd = NTsize;
            for (j = i; j < dims; j++)
            {
                dd *= dimlen[j + 1];
            }
            bytetoindex[i] = dd;
        }

        totalSize = bytetoindex[0];
    }

    public ArrayDescriptor(final String[] stringArray, final int maxLength) throws HDF5Exception
    {
        theClass = stringArray.getClass();
        NT = 'L';
        className = "java.lang.String";
        NTsize = maxLength + 1;
        dims = 1;

        /* fill in the table */
        dimlen = new int[dims + 1];
        dimlen[0] = 1;
        dimlen[1] = stringArray.length;
        totalSize = dimlen[1] * NTsize;
    }

    /**
     * Debug dump
     */
    public void dumpInfo()
    {
        System.out.println("Type: " + theType);
        System.out.println("Class: " + theClass);
        System.out.println("NT: " + NT + " NTsize: " + NTsize);
        System.out.println("Array has " + dims + " dimensions (" + totalSize + " bytes)");
        int i;
        for (i = 0; i <= dims; i++)
        {
            final Class<?> tc = objs[i].getClass();
            final String ss = tc.toString();
            System.out.println(i + ":  start " + dimstart[i] + ": len " + dimlen[i] + " current "
                    + currentindex[i] + " bytetoindex " + bytetoindex[i] + " object " + objs[i]
                    + " otype " + ss);
        }
    }
}
