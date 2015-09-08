package no.imr.nmdapi.dao.file;

import java.util.Collection;
import java.util.List;
import no.imr.nmd.commons.dataset.jaxb.DatasetType;
import no.imr.nmd.commons.dataset.jaxb.DatasetsType;

/**
 * This handles generic marshall and unmarshall objects to the
 * directory structure.
 *
 * @author kjetilf
 */
public interface NMDDatasetDao {

    /**
     * Get data for a single dataset.
     *
     * @param <T>
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    <T> T get(String type, String datasetName, String... dirs);

    /**
     * Delete a single dataset.
     *
     * @param <T>
     * @param type
     * @param datasetName
     * @param removeDataset
     * @param dirs
     */
    <T> void delete(String type, String datasetName, boolean removeDataset, String... dirs);

    /**
     * Update the dataset for a single dataset.
     *
     * @param <T>
     * @param type
     * @param datasetName
     * @param dirs
     * @param data
     */
    <T> void update(String type, String datasetName, T data, String... dirs);

    /**
     * Insert a single dataset.
     *
     * @param <T>
     * @param writeRole
     * @param readRole
     * @param owner
     * @param type
     * @param datasetName
     * @param addDataset
     * @param data
     * @param dirs
     */
    <T> void insert(String writeRole, String readRole, String owner, String type, String datasetName, T data, boolean addDataset, String... dirs);

    /**
     * Does the data exist.
     *
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    boolean hasData(String type, String datasetName, String... dirs);

    /**
     * Returns a list of files for a specific series. It uses pre.data.dir to
     * get pre the directory
     *
     * @param dirs
     * @return  A list of files.
     */
    List<String> list(String... dirs);

    /**
     *  Get all datasets.
     *
     * @param dirs
     * @return
     */
    DatasetsType getDatasetsByType(String type, String... dirs);

    /**
     *  Get all datasets.
     *
     * @param dirs
     * @return
     */
    DatasetsType getDatasets(String... dirs);

    /**
     *
     * @param authorities
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    boolean hasWriteAccess(Collection<String> authorities, String type, String datasetName, String... dirs);

    /**
     *
     * @param authorities
     * @param type
     * @param datasetName
     * @param dirs
     * @return
     */
    boolean hasReadAccess(Collection<String> authorities, String type, String datasetName, String... dirs);

    /**
     *
     * @param data
     * @param dirs
     */
    void updateDataset(DatasetType data, String... dirs);

    /**
     *
     * @param <T>
     * @param type
     * @param datasetName
     * @param cruisenr
     * @return
     */
    <T> T getByCruisenr(String type, String datasetName, String cruisenr);

    /**
     *
     * @param type
     * @param datasetName
     * @param cruisenr
     * @return
     */
    boolean hasDataByCruisenr(String type, String datasetName, String cruisenr);

}
