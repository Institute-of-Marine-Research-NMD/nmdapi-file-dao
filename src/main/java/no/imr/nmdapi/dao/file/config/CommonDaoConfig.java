package no.imr.nmdapi.dao.file.config;

import no.imr.nmdapi.dao.file.NMDDatasetDao;
import no.imr.nmdapi.dao.file.NMDDatasetDaoImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Generic handling that's common to more than one dao class.
 *
 * @author kjetilf
 */
@Configuration
public class CommonDaoConfig {

    /**
     * Initalizes the dao as a spring bean.
     * @return  The biotic DAO layer.
     */
    @Bean(name = "nmdDataDao")
    public NMDDatasetDao nmdDataDao() {
        return new NMDDatasetDaoImpl();
    }

}
