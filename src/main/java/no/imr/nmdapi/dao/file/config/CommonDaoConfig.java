package no.imr.nmdapi.dao.file.config;

import no.imr.nmdapi.dao.file.NMDDatasetDao;
import no.imr.nmdapi.dao.file.NMDDatasetDaoImpl;
import no.imr.nmdapi.dao.file.NMDSeriesReferenceDao;
import no.imr.nmdapi.dao.file.NMDSeriesReferenceDaoImpl;
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
     * @return
     */
    @Bean(name = "nmdDataDao")
    public NMDDatasetDao nmdDataDao() {
        return new NMDDatasetDaoImpl();
    }

    /**
     * Initalizes the dao as a spring bean.
     * @return
     */
    @Bean(name = "nmdSeriesReferenceDao")
    public NMDSeriesReferenceDao seriesReferenceDao() {
        return new NMDSeriesReferenceDaoImpl();
    }

}
