package no.imr.nmdapi.dao.file;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import no.imr.nmd.commons.dataset.jaxb.DataTypeEnum;
import no.imr.nmdapi.dao.file.TestDatasetDao.Init;
import no.imr.nmdapi.dao.file.config.CommonDaoConfig;
import no.imr.nmdapi.exceptions.AlreadyExistsException;
import no.imr.nmdapi.exceptions.NotFoundException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author kjetilf
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {CommonDaoConfig.class, Init.class})
public class TestDatasetDao {

    @org.springframework.context.annotation.Configuration
    public static class Init {

        @Bean
        public Configuration configuration() {
            Configuration cfg = new PropertiesConfiguration();
            cfg.addProperty("app.packages", "no.imr.nmdapi.dao.file:no.imr.nmd.commons.dataset.jaxb");
            cfg.addProperty("pre.data.dir", System.getProperty("java.io.tmpdir"));
            return cfg;
        }
    }

    @Autowired
    private NMDDatasetDao nmdDataDao;

    /**
     *
     */
    @Test
    public void testInsertUpdateDelete() {
        TestType testData = new TestType();
        testData.setData("test");
        nmdDataDao.insert("writeRole", "unrestricted", "imr", DataTypeEnum.BIOTIC, "test data", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        TestType testRes = nmdDataDao.get(DataTypeEnum.BIOTIC, "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertEquals(testData.getData(), testRes.getData());
    }

    @After
    public void delete() {
        try {
            nmdDataDao.delete(DataTypeEnum.BIOTIC, "test data", true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        } catch (NotFoundException e) {

        }
    }

    /**
     *
     */
    @Test(expected = AlreadyExistsException.class)
    public void testDoubleInsert() {
        TestType testData = new TestType();
        testData.setData("test");
        nmdDataDao.insert("writeRole", "unrestricted", "imr", DataTypeEnum.BIOTIC, "test data", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        nmdDataDao.insert("writeRole", "unrestricted", "imr", DataTypeEnum.BIOTIC, "test data", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");

    }

    /**
     *
     */
    @Test
    public void testInsertTwoDatasetsOfSameType() {
        TestType testData = new TestType();
        testData.setData("test");
        nmdDataDao.insert("writeRole", "unrestricted", "imr", DataTypeEnum.BIOTIC, "test data1", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertEquals(1, nmdDataDao.getDatasetsByType(DataTypeEnum.BIOTIC, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101").getDataset().size());
        nmdDataDao.insert("writeRole", "unrestricted", "imr", DataTypeEnum.BIOTIC, "test data2", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertEquals(2, nmdDataDao.getDatasetsByType(DataTypeEnum.BIOTIC, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101").getDataset().size());
        assertNotNull(nmdDataDao.get(DataTypeEnum.BIOTIC, "test data1", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        assertNotNull(nmdDataDao.get(DataTypeEnum.BIOTIC, "test data2", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        assertEquals(2, nmdDataDao.getDatasetsByType(DataTypeEnum.BIOTIC, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101").getDataset().size());
        nmdDataDao.delete(DataTypeEnum.BIOTIC, "test data1", true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertEquals(1, nmdDataDao.getDatasetsByType(DataTypeEnum.BIOTIC, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101").getDataset().size());
        nmdDataDao.delete(DataTypeEnum.BIOTIC, "test data2", true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
    }

    /**
     * Test restrictions.
     */
    @Test
    public void testAccess() {
        Collection<String> noAuths = new ArrayList<String>();
        TestType testData = new TestType();
        testData.setData("test");
        nmdDataDao.insert("unrestricted", "unrestricted", "imr", DataTypeEnum.BIOTIC, "test data", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertTrue(nmdDataDao.hasReadAccess(noAuths, DataTypeEnum.BIOTIC, "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        assertTrue(nmdDataDao.hasWriteAccess(noAuths, DataTypeEnum.BIOTIC, "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        nmdDataDao.delete(DataTypeEnum.BIOTIC, "test data", true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        nmdDataDao.insert("Write", "Read", "imr", DataTypeEnum.BIOTIC, "test data", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        Collection<String> auths = Arrays.asList("XXX", "Read", "baz");
        assertTrue(nmdDataDao.hasReadAccess(auths, DataTypeEnum.BIOTIC, "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        assertFalse(nmdDataDao.hasWriteAccess(auths, DataTypeEnum.BIOTIC, "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        auths = Arrays.asList("XXX", "Write", "baz");
        assertFalse(nmdDataDao.hasReadAccess(auths, DataTypeEnum.BIOTIC, "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        assertTrue(nmdDataDao.hasWriteAccess(auths, DataTypeEnum.BIOTIC, "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        nmdDataDao.delete(DataTypeEnum.BIOTIC, "test data", true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
    }

}
