package test.hdf5lib;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import ch.systemsx.cisd.base.exceptions.CheckedExceptionTunnel;
import ch.systemsx.cisd.base.utilities.ResourceUtilities;

@RunWith(Suite.class)
@Suite.SuiteClasses( { TestH5.class, 
        TestH5Eregister.class, 
        TestH5Edefault.class, 
        TestH5E.class, 
        TestH5Fparams.class, TestH5Fbasic.class, TestH5F.class, 
        TestH5Gbasic.class, TestH5G.class, TestH5Giterate.class,
        TestH5Sbasic.class, TestH5S.class, 
        TestH5Tparams.class, TestH5Tbasic.class, TestH5T.class, 
        TestH5Dparams.class, TestH5D.class, TestH5Dplist.class,
        TestH5Lparams.class, TestH5Lbasic.class, TestH5Lcreate.class,
        TestH5R.class, 
        TestH5P.class, TestH5PData.class, TestH5Pfapl.class,
        TestH5A.class, 
        TestH5Oparams.class, TestH5Obasic.class, TestH5Ocopy.class, TestH5Ocreate.class,
        TestH5Z.class
})

public class TestAll {
    
    @BeforeClass
    public static void setUp() {
        InputStream resourceStream = null;
        try
        {
            final File dir = new File("sourceTest/java/test/hdf5lib");
            if (dir.isDirectory() == false)
            {
                dir.mkdirs();
            }
            final File file = new File(dir, "h5ex_g_iterate.hdf");
            if (file.exists() == false)
            {
                resourceStream = ResourceUtilities.class.getResourceAsStream("/h5ex_g_iterate.hdf");
                if (resourceStream == null)
                {
                    throw new IllegalArgumentException("Resource 'h5ex_g_iterate.hdf' not found.");
                }
              final OutputStream fileStream = new FileOutputStream(file);
              try
              {
                  IOUtils.copy(resourceStream, fileStream);
                  fileStream.close();
              } finally
              {
                  IOUtils.closeQuietly(fileStream);
              }
            }
        } catch (final IOException ex)
        {
            throw CheckedExceptionTunnel.wrapIfNecessary(ex);
        } finally
        {
            IOUtils.closeQuietly(resourceStream);
        }
    }
}
