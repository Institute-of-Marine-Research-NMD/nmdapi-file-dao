package no.imr.nmdapi.dao.file;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import no.imr.nmdapi.dao.file.TestDataDao.Init;
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
public class TestDataDao {

    @org.springframework.context.annotation.Configuration
    public static class Init {
        @Bean
        public Configuration configuration() {
            Configuration cfg = new PropertiesConfiguration();
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
        nmdDataDao.insert("writeRole", "unrestricted", "imr", "test", "test data", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        TestType testRes = nmdDataDao.get("test", "test data", "no.imr.nmdapi.dao.file", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertEquals(testData.getData(), testRes.getData());
    }

    /**
     * Test series data.
     */
    @Test
    public void testInsertUpdateDeleteSeries() {
        TestType testData = new TestType();
        testData.setData("test");
        nmdDataDao.insert("writeRole", "unrestricted", "imr", "Cruiseseries", "test series", testData, true, "Series");
        assertNotNull(nmdDataDao.get("Cruiseseries", "test series", "no.imr.nmdapi.dao.file", "Series"));
        nmdDataDao.delete("Cruiseseries", "test series", true, "Series");
    }

    /**
     * Test series data.
     */
    @Test
    public void testInsertUpdateDeleteReferences() {
        TestType testData = new TestType();
        testData.setData("test");
        nmdDataDao.insert("writeRole", "unrestricted", "imr", "data", "test series", testData, true, "Reference");
        assertNotNull(nmdDataDao.get("data", "test series", "no.imr.nmdapi.dao.file", "Reference"));
        nmdDataDao.delete("data", "test series", true, "Reference");
    }

    @After
    public void delete() {
        try {
            nmdDataDao.delete("test", "test data", true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        } catch(NotFoundException e) {

        }
    }

    /**
     *
     */
    @Test(expected=AlreadyExistsException.class)
    public void testDoubleInsert() {
        TestType testData = new TestType();
        testData.setData("test");
        nmdDataDao.insert("writeRole", "unrestricted", "imr", "test", "test data", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        nmdDataDao.insert("writeRole", "unrestricted", "imr", "test", "test data", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");

    }

    /**
     *
     */
    @Test
    public void testInsertTwoDatasetsOfSameType() {
        TestType testData = new TestType();
        testData.setData("test");
        //nmdDataDao.insert("writeRole", "unrestricted", "imr", "test", "test data1", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertEquals(1, nmdDataDao.getDatasetsByType("test", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101").getDataset().size());
        nmdDataDao.insert("writeRole", "unrestricted", "imr", "test", "test data2", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertEquals(2, nmdDataDao.getDatasetsByType("test", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101").getDataset().size());
        assertNotNull(nmdDataDao.get("test", "test data1", TestType.class.getPackage().getName(), "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        assertNotNull(nmdDataDao.get("test", "test data2", TestType.class.getPackage().getName(), "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        assertEquals(2, nmdDataDao.getDatasetsByType("test", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101").getDataset().size());
        nmdDataDao.delete("test", "test data1", true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertEquals(1, nmdDataDao.getDatasetsByType("test", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101").getDataset().size());
        nmdDataDao.delete("test", "test data2", true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
    }

     /**
     * Test restrictions.
     */
    @Test
    public void testAccess() {
        Collection<String> noAuths = new ArrayList<String>();
        TestType testData = new TestType();
        testData.setData("test");
        nmdDataDao.insert("unrestricted", "unrestricted", "imr", "test", "test data", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertTrue(nmdDataDao.hasReadAccess(noAuths, "test", "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        assertTrue(nmdDataDao.hasWriteAccess(noAuths, "test", "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        nmdDataDao.delete("test", "test data", true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        nmdDataDao.insert("Write", "Read", "imr", "test", "test data", testData, true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        Collection<String> auths = Arrays.asList("test", "Read", "baz");
        assertTrue(nmdDataDao.hasReadAccess(auths, "test", "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        assertFalse(nmdDataDao.hasWriteAccess(auths, "test", "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        auths = Arrays.asList("test", "Write", "baz");
        assertFalse(nmdDataDao.hasReadAccess(auths, "test", "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        assertTrue(nmdDataDao.hasWriteAccess(auths, "test", "test data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101"));
        nmdDataDao.delete("test", "test data", true, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
    }

}
