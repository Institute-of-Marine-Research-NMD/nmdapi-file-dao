package no.imr.nmdapi.dao.file;

import java.io.File;
import java.util.List;

/**
 * This handles generic marshall and unmarshall objects to the
 * directory structure.
 *
 * @author kjetilf
 */
public interface NMDDataDao {

    /**
     *
     * @param <T>
     * @param name
     * @param clazz
     * @return
     */
    <T> T get(String name, Class<T> clazz);

    /**
     *
     * @param <T>
     * @param name
     */
    <T> void delete(String name);

    /**
     *
     * @param <T>
     * @param name
     * @param data
     * @param clazz
     */
    <T> void update(String name, T data, Class<T> clazz);

    /**
     *
     * @param <T>
     * @param name
     * @param data
     * @param clazz
     */
    <T> void insert(String name, T data, Class<T> clazz);

    boolean hasData(String name);

     /**
     * Return data.
     *
     * @param <T>   Type of class to return.
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @return              Mission object.
     */
    <T> T get(String missiontype, String year, String platform, String delivery, Class<T> clazz);

    /**
     * Delete biotic data for mission.
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     */
    <T> void delete(String missiontype, String year, String platform, String delivery);

    /**
     * Update.
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @param data
     *
     */
    <T> void update(String missiontype, String year, String platform, String delivery, T data, Class<T> clazz);

    /**
     * Insert data.
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @param data   data.
     */
    <T> void insert(String missiontype, String year, String platform, String delivery, T data, Class<T> clazz);


    /**
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @return  True if data exists.
     */
    boolean hasData(String missiontype, String year, String platform, String delivery);

    /**
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @param type
     * @return
     */
    void insertDataset(String missiontype, String year, String platform, String delivery, String type);

    /**
     *
     * @param missiontype
     * @param year
     * @param platform
     * @param delivery
     * @param type
     * @return
     */
    void deleteDataset(String missiontype, String year, String platform, String delivery, String type);

    /**
     * Returns a list of files for a specific series. It uses pre.data.dir to
     * get pre the directory
     *
     * @param args  Arguments as directory names.
     * @return  A list of files.
     */
    List<String> listSeries(String... args);

    /**
     *
     * @param clazz
     * @param cruisenr  Cruise number.
     * @return
     */
    Object getByCruiseNr(Class<?> clazz, String cruisenr);

}
