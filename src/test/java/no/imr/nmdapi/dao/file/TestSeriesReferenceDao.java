package no.imr.nmdapi.dao.file;

import no.imr.nmdapi.dao.file.TestSeriesReferenceDao.Init;
import no.imr.nmdapi.dao.file.config.CommonDaoConfig;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import static org.junit.Assert.assertEquals;
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
public class TestSeriesReferenceDao {

    @org.springframework.context.annotation.Configuration
    public static class Init {
        @Bean
        public Configuration configuration() {
            Configuration cfg = new PropertiesConfiguration();
            cfg.addProperty("pre.data.dir", System.getProperty("java.io.tmpdir"));
            cfg.addProperty("app.packages", "no.imr.nmdapi.dao.file:no.imr.nmd.commons.dataset.jaxb");
            return cfg;
        }
    }

    @Autowired
    private NMDSeriesReferenceDao seriesReferenceDao;

    /**
     *
     */
    @Test
    public void testInsertUpdateDelete() {
        TestType testData = new TestType();
        testData.setData("test");
        if (seriesReferenceDao.hasData("test data")) {
            seriesReferenceDao.delete("test", "test data", true);
        }
        seriesReferenceDao.insert("writeRole", "unrestricted", "imr", "test", "test data", testData, true);
        TestType testRes = seriesReferenceDao.get("test data");
        assertEquals(testData.getData(), testRes.getData());
    }


}
