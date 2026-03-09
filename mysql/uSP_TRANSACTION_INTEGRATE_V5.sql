use TW_REAL_ESTATE_DB;

/*
-- 相關查詢資源擴大
SET GLOBAL net_read_timeout = 6000;
SET GLOBAL net_write_timeout = 6000;
SET GLOBAL max_allowed_packet = 1073741824;
SET GLOBAL connect_timeout = 60;
-- 查詢限制設定擴大
SET GLOBAL max_execution_time = 0;
SET GLOBAL group_concat_max_len = 1073741824;
-- 時間相關擴大
SET GLOBAL wait_timeout = 28800;
SET GLOBAL interactive_timeout = 28800;
--
SHOW GLOBAL VARIABLES LIKE '%timeout%';
SHOW GLOBAL VARIABLES LIKE 'max_allowed_packet';
SHOW GLOBAL VARIABLES LIKE 'group_concat_max_len';
select length('INTERIOR_WEB_RE_TRANSACTION_20250814_CHECK_LIST')
*/

DROP PROCEDURE IF EXISTS uSP_TRANSACTION_INTEGRATE;
DELIMITER $$
CREATE DEFINER=`root`@`%` PROCEDURE `uSP_TRANSACTION_INTEGRATE`(OUT RESULT_MSG VARCHAR(100))
BEGIN
-- STEP. 0 所有變數
	DECLARE sql_count_check           INT;
    DECLARE transaction_tb_date       VARCHAR(8);
    DECLARE main_tb_name              VARCHAR(42);
    DECLARE sub_tb_name               VARCHAR(42);
    DECLARE begin_int                 INT;
    DECLARE end_int                   INT;

    DECLARE exec_date                 VARCHAR(100);
    DECLARE cursor_tb_city_name       VARCHAR(100);
    DECLARE cursor_tb_district_name   VARCHAR(100);
    DECLARE main_city_district_count  INT;
    DECLARE sub_city_district_count   INT;
    DECLARE total_union_row_count     INT;

    DECLARE sub_check_tb_name         VARCHAR(47);

    DECLARE v_error_msg               TEXT DEFAULT NULL;
    DECLARE v_log_text                VARCHAR(500);

    /* 例外處理：任何 SQL 例外 → 記錄錯誤並回滾 */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 v_error_msg = MESSAGE_TEXT;
        INSERT INTO ADM_ERROR_LOG (SP_NAME, DATA_DT, TEXT, ERR_MSG, CREATE_DT, CREATE_BY)
        VALUES ('uSP_TRANSACTION_INTEGRATE', NOW(), v_log_text, v_error_msg, NOW(), 'SYSTEM');
        ROLLBACK;
    END;

    /* =========================
       程式說明
       1) DAY_BY_MONTH 的爬蟲完成後呼叫
       2) 將動態 yyyyMMdd_STAGE 與 DATA/ 或 FULL_STAGE 整合
       3) 每次整合寫入 INTERIOR_WEB_RE_TRANSACTION_DATA_RECORD
       ========================= */

    /* STEP 1. 檢查 DATA 層是否為空（第一次跑） */
    SELECT COUNT(*) INTO sql_count_check FROM INTERIOR_WEB_RE_TRANSACTION_DATA;

    IF sql_count_check = 0 THEN
        /* ========= 第一次跑：FULL_STAGE ∪ 當日_STAGE ========= */
        SELECT DATE_FORMAT(NOW(), '%Y%m%d') INTO transaction_tb_date;
        SET main_tb_name = 'INTERIOR_WEB_RE_TRANSACTION_FULL_STAGE';
        SET sub_tb_name  = CONCAT('INTERIOR_WEB_RE_TRANSACTION_', transaction_tb_date, '_STAGE');

        /* 以 FULL_CHECK_LIST的迴圈 */
        SELECT MIN(RECORD_ID) INTO begin_int FROM INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST;
        SELECT MAX(RECORD_ID) INTO end_int   FROM INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST;

        SELECT DATE_FORMAT(NOW(), '%Y-%m-%d') INTO exec_date;

        WHILE begin_int <= end_int DO
            /* 取出本圈城市/區 */
            SELECT CITY_NAME INTO cursor_tb_city_name FROM INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST WHERE RECORD_ID = begin_int;
            SELECT DISTRICT_NAME INTO cursor_tb_district_name FROM INTERIOR_WEB_RE_TRANSACTION_FULL_CHECK_LIST WHERE RECORD_ID = begin_int;

            SET v_log_text = CONCAT('LOOP: ', cursor_tb_city_name, '/', cursor_tb_district_name);

            /*DEBUG用*/
            IF begin_int = 1 OR begin_int % 25 = 0 OR begin_int = 365 THEN
                SELECT CONCAT('CURRENT LOOP: ', begin_int, ' city: ', cursor_tb_city_name, ', district: ', cursor_tb_district_name) AS DEBUG_LOG;
            END IF;

            /* 變數提供給動態SQL的參數 */
            SET @city := cursor_tb_city_name;
            SET @district := cursor_tb_district_name;

            /* 主/副表筆數 */
            SET @sql_main = CONCAT('SELECT COUNT(*) INTO @tmp_main_count FROM ', main_tb_name,
                                   ' WHERE city_name = ? AND district_name = ?');
            PREPARE stmt_main FROM @sql_main;
            EXECUTE stmt_main USING @city, @district;
            DEALLOCATE PREPARE stmt_main;
            SELECT @tmp_main_count INTO main_city_district_count;

            SET @sql_sub = CONCAT('SELECT COUNT(*) INTO @tmp_sub_count FROM ', sub_tb_name,
                                  ' WHERE city_name = ? AND district_name = ?');
            PREPARE stmt_sub FROM @sql_sub;
            EXECUTE stmt_sub USING @city, @district;
            DEALLOCATE PREPARE stmt_sub;
            SELECT @tmp_sub_count INTO sub_city_district_count;

            /* 組合 UNION ALL */
            SET @sql_union = CONCAT(
                'SELECT CITY_NAME, DISTRICT_NAME, ADDRESS, COMMUNITY, TOTAL_PRICE_10K, TRANSACTION_DATE, ',
                'UNIT_PRICE, TOTAL_TSUBO, REAL_SPACE_PERCENT, TYPE, HOUSE_AGE, LEVEL, MAIN_PURPOSE, ',
                'TRANSACTION_TARGET, BUILDING_STRUCTURE, CAR_PARKING_PRICE_10K, MANAGE_UNIT_YN, ELEVATOR_YN, ',
                'NOTE, ', QUOTE(NOW()), ', ', QUOTE('uSP_TRANSACTION_INTEGRATE'), ' FROM ', main_tb_name,
                ' WHERE CITY_NAME = ''', @city, ''' AND DISTRICT_NAME = ''', @district, ''' ',
                ' UNION ALL ',
                'SELECT CITY_NAME, DISTRICT_NAME, ADDRESS, COMMUNITY, TOTAL_PRICE_10K, TRANSACTION_DATE, ',
                'UNIT_PRICE, TOTAL_TSUBO, REAL_SPACE_PERCENT, TYPE, HOUSE_AGE, LEVEL, MAIN_PURPOSE, ',
                'TRANSACTION_TARGET, BUILDING_STRUCTURE, CAR_PARKING_PRICE_10K, MANAGE_UNIT_YN, ELEVATOR_YN, ',
                'NOTE, ', QUOTE(NOW()), ', ', QUOTE('uSP_TRANSACTION_INTEGRATE'), ' FROM ', sub_tb_name,
                ' WHERE CITY_NAME = ''', @city, ''' AND DISTRICT_NAME = ''', @district, ''' '
            );

            /* 建立TMP_UNION暫存表 */
            SET @sql_drop_tmp := 'DROP TEMPORARY TABLE IF EXISTS TMP_UNION';
            PREPARE stmt_drop_tmp FROM @sql_drop_tmp; EXECUTE stmt_drop_tmp; DEALLOCATE PREPARE stmt_drop_tmp;

            SET @sql_make_tmp = CONCAT('CREATE TEMPORARY TABLE TMP_UNION AS ', @sql_union);
            PREPARE stmt_make_tmp FROM @sql_make_tmp; EXECUTE stmt_make_tmp; DEALLOCATE PREPARE stmt_make_tmp;

            /* 計算筆數 */
            SELECT COUNT(*) INTO total_union_row_count FROM TMP_UNION;

            /* 交易開始：刪除後回灌(僅限本迴圈城市/鄉鎮區) */
            START TRANSACTION;

            IF total_union_row_count > 0 THEN
                DELETE t
                FROM INTERIOR_WEB_RE_TRANSACTION_DATA t
                JOIN (SELECT DISTINCT CITY_NAME, DISTRICT_NAME FROM TMP_UNION) d
                  ON t.CITY_NAME = d.CITY_NAME
                 AND t.DISTRICT_NAME = d.DISTRICT_NAME;

                INSERT INTO INTERIOR_WEB_RE_TRANSACTION_DATA (CITY_NAME, DISTRICT_NAME, ADDRESS, COMMUNITY, TOTAL_PRICE_10K, TRANSACTION_DATE,UNIT_PRICE, TOTAL_TSUBO, REAL_SPACE_PERCENT, TYPE, HOUSE_AGE, LEVEL,MAIN_PURPOSE, TRANSACTION_TARGET, BUILDING_STRUCTURE, CAR_PARKING_PRICE_10K,MANAGE_UNIT_YN, ELEVATOR_YN, NOTE, UPDATE_TIME, UPDATE_BY)
                SELECT CITY_NAME, DISTRICT_NAME, ADDRESS, COMMUNITY, TOTAL_PRICE_10K, TRANSACTION_DATE, UNIT_PRICE, TOTAL_TSUBO, REAL_SPACE_PERCENT, TYPE, HOUSE_AGE, LEVEL, MAIN_PURPOSE, TRANSACTION_TARGET, BUILDING_STRUCTURE, CAR_PARKING_PRICE_10K, MANAGE_UNIT_YN, ELEVATOR_YN, NOTE, UPDATE_TIME, UPDATE_BY FROM TMP_UNION;
            END IF;
            COMMIT;
            /* 刪除暫存表 */
            DROP TEMPORARY TABLE IF EXISTS TMP_UNION;
            /* 寫入 RECORD */
           INSERT INTO INTERIOR_WEB_RE_TRANSACTION_DATA_RECORD(EXEC_DATE, MAIN_TABLE, MAIN_TABLE_CITY_NAME, MAIN_TABLE_DISTRICT_NAME, MAIN_TABLE_ROW_COUNT, SUB_TABLE, SUB_TABLE_CITY_NAME, SUB_TABLE_DISTRICT_NAME, SUB_TABLE_ROW_COUNT, TOTAL_ROW_COUNT, DONE_YN, UPDATE_TIME, UPDATE_BY)
           VALUES(exec_date, main_tb_name, cursor_tb_city_name, cursor_tb_district_name, main_city_district_count, sub_tb_name, cursor_tb_city_name, cursor_tb_district_name, sub_city_district_count, total_union_row_count, CONCAT('Y_', transaction_tb_date,'_D_OF_MONTH_RUN'), now(), 'uSP_TRANSACTION_INTEGRATE');
            /* 下一圈 */
            SET begin_int = begin_int + 1;
            DO SLEEP(0.5);
        END WHILE;

        INSERT INTO ADM_LOG(SP_NAME, DATA_DT, DONE_YN, CREATE_DT, CREATE_BY)
        VALUES('uSP_TRANSACTION_INTEGRATE', NOW(), 'Y', DATE_FORMAT(NOW(), '%Y-%m-%d'), 'SYSTEM');

        SET RESULT_MSG = CONCAT('FIRST_TIME_RUN_', transaction_tb_date, '_DONE');

    ELSE
        /* ========= DATA當日_STAGE ========= */
        SELECT DATE_FORMAT(NOW(), '%Y%m%d') INTO transaction_tb_date;
        SET main_tb_name = 'INTERIOR_WEB_RE_TRANSACTION_DATA';
        SET sub_tb_name = CONCAT('INTERIOR_WEB_RE_TRANSACTION_', transaction_tb_date, '_STAGE');
        SET sub_check_tb_name = CONCAT('INTERIOR_WEB_RE_TRANSACTION_', transaction_tb_date, '_CHECK_LIST');

        /* 以當日 CHECK_LIST 範圍迴圈 */
        SET @sql_min = CONCAT('SELECT MIN(RECORD_ID) INTO @min_id FROM ', sub_check_tb_name);
        PREPARE stmt_min FROM @sql_min; EXECUTE stmt_min; DEALLOCATE PREPARE stmt_min;

        SET @sql_max = CONCAT('SELECT MAX(RECORD_ID) INTO @max_id FROM ', sub_check_tb_name);
        PREPARE stmt_max FROM @sql_max; EXECUTE stmt_max; DEALLOCATE PREPARE stmt_max;

        SET begin_int = @min_id;
        SET end_int = @max_id;

        SELECT DATE_FORMAT(NOW(), '%Y-%m-%d') INTO exec_date;
        /* 重跑防呆：清掉當天非首次紀錄 */
        DELETE FROM INTERIOR_WEB_RE_TRANSACTION_DATA_RECORD
        WHERE EXEC_DATE = exec_date
          AND DONE_YN  != 'Y_FIRST_TIME_RUN';

        WHILE begin_int <= end_int DO
            SET @record_id := begin_int;

            /* 取出城市 */
            SET @sql_city = CONCAT('SELECT CITY_NAME INTO @tmp_city_name FROM ', sub_check_tb_name, ' WHERE RECORD_ID = ?');
            PREPARE stmt_city FROM @sql_city; EXECUTE stmt_city USING @record_id; DEALLOCATE PREPARE stmt_city;
            SET cursor_tb_city_name = @tmp_city_name;

            /* 取出區 */
            SET @sql_dist = CONCAT('SELECT DISTRICT_NAME INTO @tmp_district_name FROM ', sub_check_tb_name, ' WHERE RECORD_ID = ?');
            PREPARE stmt_dist FROM @sql_dist; EXECUTE stmt_dist USING @record_id; DEALLOCATE PREPARE stmt_dist;
            SET cursor_tb_district_name = @tmp_district_name;

            SET v_log_text = CONCAT('LOOP: ', cursor_tb_city_name, '/', cursor_tb_district_name);

            IF begin_int = 1 OR begin_int % 25 = 0 OR begin_int = 365 THEN
                SELECT CONCAT('CURRENT LOOP: ', begin_int, ' city: ', cursor_tb_city_name, ', district: ', cursor_tb_district_name) AS DEBUG_LOG;
            END IF;

            SET @city := cursor_tb_city_name;
            SET @district := cursor_tb_district_name;

            /* 主/副表筆數 */
            SET @sql_main = CONCAT('SELECT COUNT(*) INTO @tmp_main_count FROM ', main_tb_name,
                                   ' WHERE city_name = ? AND district_name = ?');
            PREPARE stmt_main FROM @sql_main; EXECUTE stmt_main USING @city, @district; DEALLOCATE PREPARE stmt_main;
            SELECT @tmp_main_count INTO main_city_district_count;

            SET @sql_sub  = CONCAT('SELECT COUNT(*) INTO @tmp_sub_count FROM ', sub_tb_name,
                                   ' WHERE city_name = ? AND district_name = ?');
            PREPARE stmt_sub FROM @sql_sub; EXECUTE stmt_sub USING @city, @district; DEALLOCATE PREPARE stmt_sub;
            SELECT @tmp_sub_count INTO sub_city_district_count;

            /* 組合 UNION ALL（字串） */
            SET @sql_union = CONCAT(
                'SELECT CITY_NAME, DISTRICT_NAME, ADDRESS, COMMUNITY, TOTAL_PRICE_10K, TRANSACTION_DATE, ',
                'UNIT_PRICE, TOTAL_TSUBO, REAL_SPACE_PERCENT, TYPE, HOUSE_AGE, LEVEL, MAIN_PURPOSE, ',
                'TRANSACTION_TARGET, BUILDING_STRUCTURE, CAR_PARKING_PRICE_10K, MANAGE_UNIT_YN, ELEVATOR_YN, ',
                'NOTE, ', QUOTE(NOW()), ', ', QUOTE('uSP_TRANSACTION_INTEGRATE'), ' FROM ', main_tb_name,
                ' WHERE CITY_NAME = ''', @city, ''' AND DISTRICT_NAME = ''', @district, ''' ',
                ' UNION ALL ',
                'SELECT CITY_NAME, DISTRICT_NAME, ADDRESS, COMMUNITY, TOTAL_PRICE_10K, TRANSACTION_DATE, ',
                'UNIT_PRICE, TOTAL_TSUBO, REAL_SPACE_PERCENT, TYPE, HOUSE_AGE, LEVEL, MAIN_PURPOSE, ',
                'TRANSACTION_TARGET, BUILDING_STRUCTURE, CAR_PARKING_PRICE_10K, MANAGE_UNIT_YN, ELEVATOR_YN, ',
                'NOTE, ', QUOTE(NOW()), ', ', QUOTE('uSP_TRANSACTION_INTEGRATE'), ' FROM ', sub_tb_name,
                ' WHERE CITY_NAME = ''', @city, ''' AND DISTRICT_NAME = ''', @district, ''' '
            );

            /* 建立 TMP_UNION 暫存表 */
            SET @sql_drop_tmp := 'DROP TEMPORARY TABLE IF EXISTS TMP_UNION';
            PREPARE stmt_drop_tmp FROM @sql_drop_tmp; EXECUTE stmt_drop_tmp; DEALLOCATE PREPARE stmt_drop_tmp;

            SET @sql_make_tmp = CONCAT('CREATE TEMPORARY TABLE TMP_UNION AS ', @sql_union);
            PREPARE stmt_make_tmp FROM @sql_make_tmp; EXECUTE stmt_make_tmp; DEALLOCATE PREPARE stmt_make_tmp;

            /* 計算筆數 */
            SELECT COUNT(*) INTO total_union_row_count FROM TMP_UNION;

            /* 交易開始：刪除後回灌(僅限本迴圈城市/鄉鎮區) */
            START TRANSACTION;

            IF total_union_row_count > 0 THEN
                DELETE t
                FROM INTERIOR_WEB_RE_TRANSACTION_DATA t
                JOIN (SELECT DISTINCT CITY_NAME, DISTRICT_NAME FROM TMP_UNION) d
                  ON t.CITY_NAME = d.CITY_NAME
                 AND t.DISTRICT_NAME = d.DISTRICT_NAME;

                INSERT INTO INTERIOR_WEB_RE_TRANSACTION_DATA (CITY_NAME, DISTRICT_NAME, ADDRESS, COMMUNITY, TOTAL_PRICE_10K, TRANSACTION_DATE, UNIT_PRICE, TOTAL_TSUBO, REAL_SPACE_PERCENT, TYPE, HOUSE_AGE, LEVEL, MAIN_PURPOSE, TRANSACTION_TARGET, BUILDING_STRUCTURE, CAR_PARKING_PRICE_10K, MANAGE_UNIT_YN, ELEVATOR_YN, NOTE, UPDATE_TIME, UPDATE_BY)
                SELECT CITY_NAME, DISTRICT_NAME, ADDRESS, COMMUNITY, TOTAL_PRICE_10K, TRANSACTION_DATE, UNIT_PRICE, TOTAL_TSUBO, REAL_SPACE_PERCENT, TYPE, HOUSE_AGE, LEVEL, MAIN_PURPOSE, TRANSACTION_TARGET, BUILDING_STRUCTURE, CAR_PARKING_PRICE_10K, MANAGE_UNIT_YN, ELEVATOR_YN, NOTE, UPDATE_TIME, UPDATE_BY FROM TMP_UNION;
            END IF;

            COMMIT;

            /* 丟棄暫存表 */
            DROP TEMPORARY TABLE IF EXISTS TMP_UNION;
            /*寫入 RECORD*/
            INSERT INTO INTERIOR_WEB_RE_TRANSACTION_DATA_RECORD(EXEC_DATE, MAIN_TABLE, MAIN_TABLE_CITY_NAME, MAIN_TABLE_DISTRICT_NAME, MAIN_TABLE_ROW_COUNT, SUB_TABLE, SUB_TABLE_CITY_NAME, SUB_TABLE_DISTRICT_NAME, SUB_TABLE_ROW_COUNT, TOTAL_ROW_COUNT, DONE_YN, UPDATE_TIME, UPDATE_BY)
            VALUES(exec_date, main_tb_name, cursor_tb_city_name, cursor_tb_district_name, main_city_district_count, sub_tb_name, cursor_tb_city_name, cursor_tb_district_name, sub_city_district_count, total_union_row_count, CONCAT('Y_', transaction_tb_date,'_D_OF_MONTH_RUN'), now(), 'uSP_TRANSACTION_INTEGRATE');
            /* 下一圈 */
            SET begin_int = begin_int + 1;
            DO SLEEP(0.5);
        END WHILE;

        INSERT INTO ADM_LOG(SP_NAME, DATA_DT, DONE_YN, CREATE_DT, CREATE_BY)
        VALUES('uSP_TRANSACTION_INTEGRATE', NOW(), 'Y', DATE_FORMAT(NOW(), '%Y-%m-%d'), 'SYSTEM');

        SET RESULT_MSG = CONCAT('DAILY_BY_MONTH_', transaction_tb_date, '_DONE');
    END IF;
END$$

DELIMITER ;
-- SET @msg = '';
-- CALL uSP_TRANSACTION_INTEGRATE(@msg);
-- SELECT @msg;