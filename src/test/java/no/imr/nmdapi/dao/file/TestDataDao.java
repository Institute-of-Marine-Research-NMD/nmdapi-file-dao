package no.imr.nmdapi.dao.file;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import no.imr.nmdapi.dao.file.TestDataDao.Init;
import no.imr.nmdapi.dao.file.config.CommonDaoConfig;
import no.imr.nmdapi.exceptions.AlreadyExistsException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
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
            cfg.addProperty("post.data.dir", "test");
            cfg.addProperty("default.readrole", "READ");
            cfg.addProperty("default.writerole", "WRITE");
            return cfg;
        }
    }

    @Autowired
    private NMDDataDao nmdDataDao;

    /**
     *
     */
    @Test
    public void testDoubleInsertUpdateDelete() {
        TestType jaxbObject = new TestType();
        jaxbObject.setData("test");
        try {
            nmdDataDao.insert("1", "2", "3", "4", jaxbObject, TestType.class);
            nmdDataDao.insert("1", "2", "3", "4", jaxbObject, TestType.class);
        } catch(AlreadyExistsException e) {

        }
        assertTrue(nmdDataDao.get("1", "2", "3", "4", TestType.class).getData().equals("test"));
        assertTrue(nmdDataDao.hasData("1", "2", "3", "4"));

        jaxbObject.setData("test2");
        nmdDataDao.update("1", "2", "3", "4", jaxbObject, TestType.class);
        assertTrue(nmdDataDao.get("1", "2", "3", "4", TestType.class).getData().equals("test2"));

        nmdDataDao.delete("1", "2", "3", "4");
        assertFalse(nmdDataDao.hasData("1", "2", "3", "4"));
    }

    /**
     *
     */
    @Test
    public void testInsertDeleteDataset() {

        nmdDataDao.insertDataset("1", "2", "3", "4", "test");

        nmdDataDao.deleteDataset("1", "2", "3", "4", "test");
    }

}
