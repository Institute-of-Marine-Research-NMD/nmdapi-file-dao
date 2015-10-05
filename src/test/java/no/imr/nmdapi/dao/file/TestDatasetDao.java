package no.imr.nmdapi.dao.file;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import no.imr.nmd.commons.dataset.jaxb.DataTypeEnum;
import no.imr.nmd.commons.dataset.jaxb.DatasetType;
import no.imr.nmd.commons.dataset.jaxb.QualityEnum;
import no.imr.nmdapi.dao.file.TestDatasetDao.Init;
import no.imr.nmdapi.dao.file.config.CommonDaoConfig;
import no.imr.nmdapi.exceptions.AlreadyExistsException;
import no.imr.nmdapi.exceptions.NotFoundException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import static org.junit.Assert.assertEquals;
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
        nmdDataDao.modifyDataset("Write", "Read", "", "imr", QualityEnum.NONE,DataTypeEnum.BIOTIC, "data", null, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        DatasetType dataset = nmdDataDao.getDatasetByName(DataTypeEnum.BIOTIC, "data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertNotNull(dataset.getId());
        assertEquals("Read", dataset.getRestrictions().getRead());
        assertEquals("Write", dataset.getRestrictions().getWrite());
        assertEquals(QualityEnum.NONE, dataset.getQualityAssured());
        nmdDataDao.modifyDataset("Write", "Read2", "", "imr", QualityEnum.NONE,DataTypeEnum.BIOTIC, "data", null, "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        DatasetType dataset2 = nmdDataDao.getDatasetByName(DataTypeEnum.BIOTIC, "data", "Forskningsdata", "2015", "G O Sars_LMEL", "2015101");
        assertEquals("Read2", dataset2.getRestrictions().getRead());
        assertEquals("Write", dataset2.getRestrictions().getWrite());
        assertEquals(QualityEnum.NONE, dataset2.getQualityAssured());
        assertEquals(dataset.getId(), dataset2.getId());

    }

}
