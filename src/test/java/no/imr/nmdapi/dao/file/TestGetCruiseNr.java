package no.imr.nmdapi.dao.file;

import java.io.File;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import no.imr.nmdapi.dao.file.TestGetCruiseNr.Init;
import no.imr.nmdapi.dao.file.config.CommonDaoConfig;
import no.imr.nmdapi.exceptions.AlreadyExistsException;
import no.imr.nmdapi.exceptions.NotFoundException;
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
public class TestGetCruiseNr {

 @org.springframework.context.annotation.Configuration
    public static class Init {
        @Bean
        public Configuration configuration() {
            Configuration cfg = new PropertiesConfiguration();
            cfg.addProperty("pre.data.dir", System.getProperty("java.io.tmpdir").concat(File.separator).concat("pre"));
            cfg.addProperty("post.data.dir", "post");
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
    public void testGetByCruiseNr() {
        TestType jaxbObject = new TestType();
        jaxbObject.setData("test");
        try {
            nmdDataDao.insert("1","2015", "GOSars_LMEL", "2015101", jaxbObject, TestType.class);
        } catch(AlreadyExistsException e) {

        }
        assertNotNull(nmdDataDao.getByCruiseNr(TestType.class, "2015101"));
        nmdDataDao.delete("1","2015", "GOSars_LMEL", "2015101");
        assertFalse(nmdDataDao.hasData("1"));
    }

    /**
     *
     */
    @Test(expected=NotFoundException.class)
    public void testNotFoundGetByCruiseNr() {
        TestType jaxbObject = new TestType();
        jaxbObject.setData("test");
        try {
            nmdDataDao.insert("1","2015", "GOSars_LMEL", "2015101", jaxbObject, TestType.class);
        } catch(AlreadyExistsException e) {

        }
        assertNotNull(nmdDataDao.getByCruiseNr(TestType.class, "2016101"));
        nmdDataDao.delete("2015101");
        assertFalse(nmdDataDao.hasData("1"));
    }

}
