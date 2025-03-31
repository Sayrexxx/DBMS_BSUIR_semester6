CREATE OR REPLACE PROCEDURE compare_schemas(
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2
)   AUTHID CURRENT_USER
IS
    v_diff_count NUMBER;
    v_dev_ddl        CLOB;
    v_prod_ddl       CLOB;
    v_count          NUMBER;
    v_table_name     VARCHAR2(128);

    TYPE t_varchar_table IS TABLE OF VARCHAR2(128);
    v_affected_tables t_varchar_table := t_varchar_table();

    TYPE t_issue_map IS TABLE OF VARCHAR2(10) INDEX BY VARCHAR2(128);
    v_issue_map t_issue_map;

    TYPE t_dep_count_map IS TABLE OF NUMBER INDEX BY VARCHAR2(128);
    v_dep_count t_dep_count_map;

    TYPE t_children_map IS TABLE OF t_varchar_table INDEX BY VARCHAR2(128);
    v_children t_children_map;

    v_sorted         t_varchar_table := t_varchar_table();
    v_sorted_count   PLS_INTEGER := 0;

    CURSOR c_missing_tables IS
        SELECT table_name
        FROM all_tables
        WHERE owner = dev_schema_name
        MINUS
        SELECT table_name
        FROM all_tables
        WHERE owner = prod_schema_name;

    CURSOR c_common_tables IS
        SELECT table_name
        FROM all_tables
        WHERE owner = dev_schema_name
        INTERSECT
        SELECT table_name
        FROM all_tables
        WHERE owner = prod_schema_name;

    CURSOR c_columns_diff(p_table_name VARCHAR2) IS
        (
          SELECT 'DEV' AS src, column_name, data_type, data_length, data_precision, data_scale, nullable
          FROM all_tab_columns
          WHERE owner = dev_schema_name
            AND table_name = p_table_name
          MINUS
          SELECT 'DEV', column_name, data_type, data_length, data_precision, data_scale, nullable
          FROM all_tab_columns
          WHERE owner = prod_schema_name
            AND table_name = p_table_name
        )
        UNION ALL
        (
          SELECT 'PROD' AS src, column_name, data_type, data_length, data_precision, data_scale, nullable
          FROM all_tab_columns
          WHERE owner = prod_schema_name
            AND table_name = p_table_name
          MINUS
          SELECT 'PROD', column_name, data_type, data_length, data_precision, data_scale, nullable
          FROM all_tab_columns
          WHERE owner = dev_schema_name
            AND table_name = p_table_name
        );

    PROCEDURE check_circular_deps(
        p_schema_name IN VARCHAR2,
        p_schema_label IN VARCHAR2
    ) IS
        v_has_cycle BOOLEAN := FALSE;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('  Checking ' || p_schema_label || ' schema...');

        FOR rec IN (
            WITH fk_graph AS (
                SELECT
                    LEAST(a.table_name, b.table_name) AS table1,
                    GREATEST(a.table_name, b.table_name) AS table2,
                    MIN(c.column_name) AS column1,
                    MIN(d.column_name) AS column2
                FROM all_constraints a
                JOIN all_constraints b
                    ON a.r_constraint_name = b.constraint_name
                    AND a.owner = b.owner
                JOIN all_cons_columns c
                    ON a.owner = c.owner
                    AND a.constraint_name = c.constraint_name
                JOIN all_cons_columns d
                    ON b.owner = d.owner
                    AND b.constraint_name = d.constraint_name
                WHERE a.owner = p_schema_name
                    AND a.constraint_type = 'R'
                    AND b.constraint_type = 'P'
                GROUP BY
                    LEAST(a.table_name, b.table_name),
                    GREATEST(a.table_name, b.table_name)
                HAVING COUNT(DISTINCT a.constraint_name) > 1
            )
            SELECT * FROM fk_graph
        ) LOOP
            v_has_cycle := TRUE;
            DBMS_OUTPUT.PUT_LINE('    - ERROR: Circular dependency detected:');
            DBMS_OUTPUT.PUT_LINE('      ' || rec.table1 || '.' || rec.column1 ||
                               ' <-> ' || rec.table2 || '.' || rec.column2);
        END LOOP;

        IF NOT v_has_cycle THEN
            DBMS_OUTPUT.PUT_LINE('    - No circular dependencies found.');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('    - Error checking dependencies: ' || SQLERRM);
    END check_circular_deps;

    PROCEDURE check_cycles(
    p_schema_name IN VARCHAR2,
    p_schema_label IN VARCHAR2
) IS
    TYPE t_table_list IS TABLE OF VARCHAR2(128);
    v_path        t_table_list := t_table_list(); -- Текущий путь
    v_visited     t_table_list := t_table_list(); -- Таблицы, которые уже проверены
    v_has_cycle   BOOLEAN := FALSE;

    -- Курсор для получения списка всех таблиц схемы
    CURSOR table_cursor IS
        SELECT object_name
        FROM all_objects
        WHERE object_type = 'TABLE'
          AND owner = p_schema_name
        ORDER BY created; -- Сортировка по времени создания таблиц

    -- Функция для проверки наличия таблицы в списке
    FUNCTION is_in_list(tbl_name VARCHAR2, lst t_table_list) RETURN BOOLEAN IS
    BEGIN
        FOR i IN 1 .. lst.COUNT LOOP
            IF lst(i) = tbl_name THEN
                RETURN TRUE;
            END IF;
        END LOOP;
        RETURN FALSE;
    END is_in_list;

    -- Рекурсивная процедура для проверки циклов
    PROCEDURE explore(v_table VARCHAR2) IS
        v_cycle_start NUMBER := 0;
        v_cycle_path  VARCHAR2(4000);
        v_parent_list t_table_list;
    BEGIN
        -- Проверяем, есть ли таблица в текущем пути (обнаружение цикла)
        FOR i IN 1 .. v_path.COUNT LOOP
            IF v_path(i) = v_table THEN
                v_cycle_start := i;
                EXIT;
            END IF;
        END LOOP;

        -- Если цикл найден
        IF v_cycle_start > 0 THEN
            v_has_cycle := TRUE;
            v_cycle_path := v_path(v_cycle_start);
            FOR j IN v_cycle_start+1 .. v_path.COUNT LOOP
                v_cycle_path := v_cycle_path || ' -> ' || v_path(j);
            END LOOP;
            v_cycle_path := v_cycle_path || ' -> ' || v_table;

            DBMS_OUTPUT.PUT_LINE('    - ERROR: Circular dependency detected: ' || v_cycle_path);
            RETURN;
        END IF;

        -- Проверяем, посещали ли уже эту таблицу
        IF is_in_list(v_table, v_visited) THEN
            RETURN;
        END IF;

        -- Добавляем таблицу в текущий путь и в список посещённых
        v_path.EXTEND;
        v_path(v_path.LAST) := v_table;
        v_visited.EXTEND;
        v_visited(v_visited.LAST) := v_table;

        -- Получаем родительские таблицы через внешние ключи
        SELECT b.table_name BULK COLLECT INTO v_parent_list
        FROM all_constraints a
        JOIN all_constraints b ON a.r_constraint_name = b.constraint_name
        WHERE a.owner = p_schema_name AND a.constraint_type = 'R'
          AND a.table_name = v_table
        ORDER BY a.last_change;

        -- Рекурсивно проверяем родительские таблицы
        FOR i IN 1 .. v_parent_list.COUNT LOOP
            explore(v_parent_list(i));
        END LOOP;

        -- Убираем текущую таблицу из пути после рекурсии
        v_path.DELETE(v_path.LAST);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL; -- Если у таблицы нет родителей, ничего не делаем
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('    - Error in explore(' || v_table || '): ' || SQLERRM);
    END explore;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Step 8: Checking for complex circular dependencies...');
    DBMS_OUTPUT.PUT_LINE('  Checking ' || p_schema_label || ' schema...');

    -- Проходим по всем таблицам схемы
    FOR rec IN table_cursor LOOP
        IF NOT is_in_list(rec.object_name, v_visited) THEN
            DBMS_OUTPUT.PUT_LINE('  Starting cycle check from: ' || rec.object_name);
            explore(rec.object_name);
        END IF;
    END LOOP;

    -- Если циклов нет
    IF NOT v_has_cycle THEN
        DBMS_OUTPUT.PUT_LINE('    - No circular dependencies found.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('    - Error checking dependencies: ' || SQLERRM);
END check_cycles;



    FUNCTION normalize_ddl(p_ddl IN CLOB) RETURN CLOB IS
    v_normalized CLOB;
    BEGIN
        v_normalized := p_ddl;
        v_normalized := REPLACE(v_normalized, CHR(10), ' ');
        v_normalized := REPLACE(v_normalized, CHR(13), ' ');
        v_normalized := REGEXP_REPLACE(v_normalized, '\s+', ' ');
        v_normalized := TRIM(v_normalized);
        v_normalized := REGEXP_REPLACE(v_normalized, '"[^"]+"\.', '');
        v_normalized := REGEXP_REPLACE(v_normalized, 'EDITIONABLE', '');
        v_normalized := REGEXP_REPLACE(v_normalized, 'END\s+\w+;', 'END;');
        RETURN v_normalized;
    EXCEPTION
        WHEN OTHERS THEN
          RETURN p_ddl;
    END normalize_ddl;

    FUNCTION replace_schema(p_ddl IN CLOB) RETURN CLOB IS
    BEGIN
        RETURN REPLACE(p_ddl, '"' || UPPER(dev_schema_name) || '"', '"' || UPPER(prod_schema_name) || '"');
    END replace_schema;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting schema comparison between DEV (' || dev_schema_name || ') and PROD (' || prod_schema_name || ')');
    DBMS_OUTPUT.PUT_LINE('==================================================================');

    DBMS_OUTPUT.PUT_LINE('Step 1: Checking for missing tables in PROD schema...');
    FOR tbl IN c_missing_tables LOOP
        DBMS_OUTPUT.PUT_LINE('  - Table "' || tbl.table_name || '" is missing in PROD schema.');
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Step 2: Checking for structural differences in tables...');
    FOR tbl IN c_common_tables LOOP
        v_diff_count := 0;

        FOR col IN c_columns_diff(tbl.table_name) LOOP
            IF v_diff_count = 0 THEN
                DBMS_OUTPUT.PUT_LINE('  - Differences found in table "' || tbl.table_name || '":');
            END IF;

            DECLARE
                v_data_type VARCHAR2(100) := col.data_type;
            BEGIN
                IF col.data_type IN ('NUMBER', 'FLOAT') THEN
                    IF col.data_precision IS NOT NULL THEN
                        v_data_type := v_data_type || '(' || col.data_precision ||
                                      NVL2(col.data_scale, ',' || col.data_scale, '') || ')';
                    END IF;
                ELSE
                    v_data_type := v_data_type || '(' || col.data_length || ')';
                END IF;

                DBMS_OUTPUT.PUT_LINE('    * Column "' || col.column_name ||
                                   '" differs: ' || col.src || ' has ' ||
                                   v_data_type);
            END;

            v_diff_count := v_diff_count + 1;
        END LOOP;

        IF v_diff_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('  - No differences found in table "' || tbl.table_name || '".');
        END IF;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Step 3: Checking for missing procedures in PROD schema...');
    BEGIN
        FOR obj IN (SELECT OBJECT_NAME
                FROM ALL_OBJECTS
                WHERE owner = 'C##DEV_SCHEMA1' and OBJECT_TYPE = 'PROCEDURE'
                MINUS
                SELECT OBJECT_NAME
                FROM ALL_OBJECTS
                WHERE owner = 'C##PROD_SCHEMA1' and OBJECT_TYPE = 'PROCEDURE')
        LOOP DBMS_OUTPUT.PUT_LINE(obj.OBJECT_NAME);
        END LOOP;
    END;

    DBMS_OUTPUT.PUT_LINE('Step 4: Checking for missing functions in PROD schema...');
    BEGIN
        FOR obj IN (SELECT OBJECT_NAME
                FROM ALL_OBJECTS
                WHERE owner = 'C##DEV_SCHEMA1' and OBJECT_TYPE = 'FUNCTION'
                MINUS
                SELECT OBJECT_NAME
                FROM ALL_OBJECTS
                WHERE owner = 'C##PROD_SCHEMA1' and OBJECT_TYPE = 'FUNCTION')
        LOOP DBMS_OUTPUT.PUT_LINE(obj.OBJECT_NAME);
        END LOOP;
    END;

    DBMS_OUTPUT.PUT_LINE('Step 5: Checking for missing indexes in PROD schema...');
    BEGIN
        FOR obj IN (SELECT OBJECT_NAME
                FROM ALL_OBJECTS
                WHERE owner = 'C##DEV_SCHEMA1' and OBJECT_TYPE = 'INDEX'
                MINUS
                SELECT OBJECT_NAME
                FROM ALL_OBJECTS
                WHERE owner = 'C##PROD_SCHEMA1' and OBJECT_TYPE = 'INDEX')
        LOOP DBMS_OUTPUT.PUT_LINE(obj.OBJECT_NAME);
        END LOOP;
    END;

    DBMS_OUTPUT.PUT_LINE('Step 6: Checking for missing packages in PROD schema...');
    BEGIN
        FOR obj IN (SELECT OBJECT_NAME
                FROM ALL_OBJECTS
                WHERE owner = 'C##DEV_SCHEMA1' and OBJECT_TYPE = 'PACKAGE'
                MINUS
                SELECT OBJECT_NAME
                FROM ALL_OBJECTS
                WHERE owner = 'C##PROD_SCHEMA1' and OBJECT_TYPE = 'PACKAGE')
        LOOP DBMS_OUTPUT.PUT_LINE(obj.OBJECT_NAME);
        END LOOP;
    END;


    DBMS_OUTPUT.PUT_LINE('Step 7: Checking for circular dependencies...');
    check_circular_deps(dev_schema_name, 'DEV');
    check_circular_deps(prod_schema_name, 'PROD');

    DBMS_OUTPUT.PUT_LINE('Step 8: Checking for complex circular dependencies...');

    FOR rec IN (
    SELECT dt.table_name,
           CASE
             WHEN pt.table_name IS NULL THEN 'MISSING'
             WHEN (SELECT COUNT(*) FROM (
                      SELECT column_name, data_type, data_length, nullable
                      FROM all_tab_columns
                      WHERE owner = UPPER(dev_schema_name)
                        AND table_name = dt.table_name
                      MINUS
                      SELECT column_name, data_type, data_length, nullable
                      FROM all_tab_columns
                      WHERE owner = UPPER(prod_schema_name)
                        AND table_name = dt.table_name
                   )) > 0 THEN 'DIFF'
           END AS issue
    FROM (SELECT table_name FROM all_tables WHERE owner = UPPER(dev_schema_name)) dt
    LEFT JOIN (SELECT table_name FROM all_tables WHERE owner = UPPER(prod_schema_name)) pt
      ON dt.table_name = pt.table_name
    WHERE pt.table_name IS NULL OR
          ((SELECT COUNT(*) FROM (
              SELECT column_name, data_type, data_length, nullable
              FROM all_tab_columns
              WHERE owner = UPPER(dev_schema_name)
                AND table_name = dt.table_name
              MINUS
              SELECT column_name, data_type, data_length, nullable
              FROM all_tab_columns
              WHERE owner = UPPER(prod_schema_name)
                AND table_name = dt.table_name
            )) > 0)
  ) LOOP
    v_affected_tables.EXTEND;
    v_affected_tables(v_affected_tables.COUNT) := rec.table_name;
    v_issue_map(rec.table_name) := rec.issue;
  END LOOP;

  FOR i IN 1 .. v_affected_tables.COUNT LOOP
    v_dep_count(v_affected_tables(i)) := 0;
    v_children(v_affected_tables(i)) := t_varchar_table();
  END LOOP;

  FOR i IN 1 .. v_affected_tables.COUNT LOOP
    v_table_name := v_affected_tables(i);
    SELECT COUNT(*) INTO v_count
    FROM all_tables
    WHERE owner = UPPER(prod_schema_name)
      AND table_name = v_table_name;
    DECLARE
      v_schema_for_fk VARCHAR2(30);
    BEGIN
      IF v_count > 0 THEN
        v_schema_for_fk := UPPER(prod_schema_name);
      ELSE
        v_schema_for_fk := UPPER(dev_schema_name);
      END IF;
      FOR fk_rec IN (
        SELECT a.table_name AS child, c.table_name AS parent
        FROM all_constraints a
        JOIN all_constraints c ON a.r_constraint_name = c.constraint_name AND a.owner = c.owner
        WHERE a.constraint_type = 'R'
          AND a.owner = v_schema_for_fk
          AND a.table_name = v_table_name
      ) LOOP
        FOR j IN 1 .. v_affected_tables.COUNT LOOP
          IF fk_rec.parent = v_affected_tables(j) THEN
            v_dep_count(v_table_name) := v_dep_count(v_table_name) + 1;
            v_children(fk_rec.parent).EXTEND;
            v_children(fk_rec.parent)(v_children(fk_rec.parent).COUNT) := v_table_name;
          END IF;
        END LOOP;
      END LOOP;
    END;
  END LOOP;

  DECLARE
    TYPE t_queue IS TABLE OF VARCHAR2(128);
    v_queue t_queue := t_queue();
    v_queue_start PLS_INTEGER := 1;
    v_queue_end   PLS_INTEGER := 0;
  BEGIN
    FOR i IN 1 .. v_affected_tables.COUNT LOOP
      IF v_dep_count(v_affected_tables(i)) = 0 THEN
        v_queue_end := v_queue_end + 1;
        v_queue.EXTEND;
        v_queue(v_queue_end) := v_affected_tables(i);
      END IF;
    END LOOP;
    WHILE v_queue_start <= v_queue_end LOOP
      DECLARE
        v_current VARCHAR2(128);
      BEGIN
        v_current := v_queue(v_queue_start);
        v_queue_start := v_queue_start + 1;
        v_sorted_count := v_sorted_count + 1;
        v_sorted.EXTEND;
        v_sorted(v_sorted_count) := v_current;
        FOR i IN 1 .. v_children(v_current).COUNT LOOP
          DECLARE
            v_child VARCHAR2(128) := v_children(v_current)(i);
          BEGIN
            v_dep_count(v_child) := v_dep_count(v_child) - 1;
            IF v_dep_count(v_child) = 0 THEN
              v_queue_end := v_queue_end + 1;
              v_queue.EXTEND;
              v_queue(v_queue_end) := v_child;
            END IF;
          END;
        END LOOP;
      END;
    END LOOP;
    IF v_sorted_count < v_affected_tables.COUNT THEN
      FOR i IN 1 .. v_affected_tables.COUNT LOOP
        IF v_dep_count(v_affected_tables(i)) > 0 THEN
          v_sorted_count := v_sorted_count + 1;
          v_sorted.EXTEND;
          v_sorted(v_sorted_count) := v_affected_tables(i);
        END IF;
      END LOOP;
    END IF;
  END;

  DBMS_OUTPUT.PUT_LINE('Перечень таблиц (либо отсутствуют в PROD, либо отличаются по структуре),');
  DBMS_OUTPUT.PUT_LINE('отсортированные по порядку создания:');
  FOR i IN 1 .. v_sorted_count LOOP
    DBMS_OUTPUT.PUT_LINE('  ' || v_sorted(i));
  END LOOP;

    DBMS_OUTPUT.PUT_LINE('Step 9: Scripts to fix differences in schemas:');

    FOR rec IN (SELECT table_name FROM all_tables WHERE owner = UPPER(dev_schema_name))
    LOOP
        BEGIN
          v_dev_ddl := DBMS_METADATA.GET_DDL('TABLE', rec.table_name, UPPER(dev_schema_name));
        EXCEPTION WHEN OTHERS THEN
          v_dev_ddl := 'NO DDL';
        END;
        BEGIN
          v_prod_ddl := DBMS_METADATA.GET_DDL('TABLE', rec.table_name, UPPER(prod_schema_name));
        EXCEPTION WHEN OTHERS THEN
          v_prod_ddl := 'NO DDL';
        END;
        IF v_prod_ddl = 'NO DDL' THEN
          DBMS_OUTPUT.PUT_LINE(replace_schema(v_dev_ddl) || ';');
        ELSIF normalize_ddl(v_dev_ddl) <> normalize_ddl(v_prod_ddl) THEN
          DBMS_OUTPUT.PUT_LINE('DROP TABLE ' || rec.table_name || ' CASCADE CONSTRAINTS;');
          DBMS_OUTPUT.PUT_LINE(replace_schema(v_dev_ddl) || ';');
        END IF;
    END LOOP;
    FOR rec IN (
        SELECT table_name FROM all_tables
        WHERE owner = UPPER(prod_schema_name)
          AND table_name NOT IN (SELECT table_name FROM all_tables WHERE owner = UPPER(dev_schema_name))
        )
        LOOP
        DBMS_OUTPUT.PUT_LINE('DROP TABLE ' || rec.table_name || ' CASCADE CONSTRAINTS;');
    END LOOP;

    FOR rec IN (SELECT object_name FROM all_objects
              WHERE owner = UPPER(dev_schema_name) AND object_type = 'PROCEDURE')
    LOOP
        BEGIN
          v_dev_ddl := DBMS_METADATA.GET_DDL('PROCEDURE', rec.object_name, UPPER(dev_schema_name));
        EXCEPTION WHEN OTHERS THEN
          v_dev_ddl := 'NO DDL';
        END;
        BEGIN
          v_prod_ddl := DBMS_METADATA.GET_DDL('PROCEDURE', rec.object_name, UPPER(prod_schema_name));
        EXCEPTION WHEN OTHERS THEN
          v_prod_ddl := 'NO DDL';
        END;
        IF v_prod_ddl = 'NO DDL' THEN
          DBMS_OUTPUT.PUT_LINE(replace_schema(v_dev_ddl) || ';');
        ELSIF normalize_ddl(v_dev_ddl) <> normalize_ddl(v_prod_ddl) THEN
          DBMS_OUTPUT.PUT_LINE('DROP PROCEDURE ' || rec.object_name || ';');
          DBMS_OUTPUT.PUT_LINE(replace_schema(v_dev_ddl) || ';');
        END IF;
    END LOOP;
    FOR rec IN (
        SELECT object_name FROM all_objects
        WHERE owner = UPPER(prod_schema_name) AND object_type = 'PROCEDURE'
          AND object_name NOT IN (
            SELECT object_name FROM all_objects
            WHERE owner = UPPER(dev_schema_name) AND object_type = 'PROCEDURE'
          )
       )
       LOOP DBMS_OUTPUT.PUT_LINE('DROP PROCEDURE ' || rec.object_name || ';');
    END LOOP;

    FOR rec IN (SELECT object_name FROM all_objects
              WHERE owner = UPPER(dev_schema_name) AND object_type = 'FUNCTION')
    LOOP
        BEGIN
          v_dev_ddl := DBMS_METADATA.GET_DDL('FUNCTION', rec.object_name, UPPER(dev_schema_name));
        EXCEPTION WHEN OTHERS THEN
          v_dev_ddl := 'NO DDL';
        END;
        BEGIN
          v_prod_ddl := DBMS_METADATA.GET_DDL('FUNCTION', rec.object_name, UPPER(prod_schema_name));
        EXCEPTION WHEN OTHERS THEN
          v_prod_ddl := 'NO DDL';
        END;
        IF v_prod_ddl = 'NO DDL' THEN
          DBMS_OUTPUT.PUT_LINE(replace_schema(v_dev_ddl) || ';');
        ELSIF normalize_ddl(v_dev_ddl) <> normalize_ddl(v_prod_ddl) THEN
          DBMS_OUTPUT.PUT_LINE('DROP FUNCTION ' || rec.object_name || ';');
          DBMS_OUTPUT.PUT_LINE(replace_schema(v_dev_ddl) || ';');
        END IF;
    END LOOP;
    FOR rec IN (
        SELECT object_name FROM all_objects
        WHERE owner = UPPER(prod_schema_name) AND object_type = 'FUNCTION'
            AND object_name NOT IN (
                SELECT object_name FROM all_objects
                WHERE owner = UPPER(dev_schema_name) AND object_type = 'FUNCTION'
            )
        )
        LOOP DBMS_OUTPUT.PUT_LINE('DROP FUNCTION ' || rec.object_name || ';');
    END LOOP;

    FOR rec IN (SELECT object_name FROM all_objects
              WHERE owner = UPPER(dev_schema_name) AND object_type = 'PACKAGE')
    LOOP
        BEGIN
          v_dev_ddl := DBMS_METADATA.GET_DDL('PACKAGE', rec.object_name, UPPER(dev_schema_name));
        EXCEPTION WHEN OTHERS THEN
          v_dev_ddl := 'NO DDL';
        END;
        BEGIN
          v_prod_ddl := DBMS_METADATA.GET_DDL('PACKAGE', rec.object_name, UPPER(prod_schema_name));
        EXCEPTION WHEN OTHERS THEN
          v_prod_ddl := 'NO DDL';
        END;
        IF v_prod_ddl = 'NO DDL' THEN
          DBMS_OUTPUT.PUT_LINE(replace_schema(v_dev_ddl) || ';');
        ELSIF normalize_ddl(v_dev_ddl) <> normalize_ddl(v_prod_ddl) THEN
          DBMS_OUTPUT.PUT_LINE('DROP PACKAGE ' || rec.object_name || ';');
          DBMS_OUTPUT.PUT_LINE(replace_schema(v_dev_ddl) || ';');
        END IF;
    END LOOP;
    FOR rec IN (
        SELECT object_name FROM all_objects
        WHERE owner = UPPER(prod_schema_name) AND object_type = 'PACKAGE'
          AND object_name NOT IN (
            SELECT object_name FROM all_objects
            WHERE owner = UPPER(dev_schema_name) AND object_type = 'PACKAGE'
          )
        )
        LOOP DBMS_OUTPUT.PUT_LINE('DROP PACKAGE ' || rec.object_name || ';');
    END LOOP;

    FOR rec IN (SELECT index_name FROM all_indexes
              WHERE owner = UPPER(dev_schema_name) AND index_name NOT LIKE 'SYS_%')
    LOOP
        BEGIN
          v_dev_ddl := DBMS_METADATA.GET_DDL('INDEX', rec.index_name, UPPER(dev_schema_name));
        EXCEPTION WHEN OTHERS THEN
          v_dev_ddl := 'NO DDL';
        END;
        BEGIN
          v_prod_ddl := DBMS_METADATA.GET_DDL('INDEX', rec.index_name, UPPER(prod_schema_name));
        EXCEPTION WHEN OTHERS THEN
          v_prod_ddl := 'NO DDL';
        END;
        IF v_prod_ddl = 'NO DDL' THEN
          DBMS_OUTPUT.PUT_LINE(replace_schema(v_dev_ddl) || ';');
        ELSIF normalize_ddl(v_dev_ddl) <> normalize_ddl(v_prod_ddl) THEN
          DBMS_OUTPUT.PUT_LINE('DROP INDEX ' || rec.index_name || ';');
          DBMS_OUTPUT.PUT_LINE(replace_schema(v_dev_ddl) || ';');
        END IF;
    END LOOP;
    FOR rec IN (
        SELECT index_name FROM all_indexes
        WHERE owner = UPPER(prod_schema_name) AND index_name NOT LIKE 'SYS_%'
          AND index_name NOT IN (
            SELECT index_name FROM all_indexes
            WHERE owner = UPPER(dev_schema_name) AND index_name NOT LIKE 'SYS_%'
          )
        )
    LOOP
        DBMS_OUTPUT.PUT_LINE('DROP INDEX ' || rec.index_name || ';');
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Schema comparison completed.');
    DBMS_OUTPUT.PUT_LINE('==================================================================');
END;
/

CREATE USER c##dev_schema1 IDENTIFIED BY dev_password;
GRANT CONNECT, RESOURCE TO c##dev_schema1;

CREATE USER c##prod_schema1 IDENTIFIED BY prod_password;
GRANT CONNECT, RESOURCE TO c##prod_schema1;

BEGIN
    EXECUTE IMMEDIATE 'ALTER SESSION SET CURRENT_SCHEMA = c##dev_schema1';
END;
/
CREATE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    name VARCHAR2(100),
    email VARCHAR2(100),
    orders_id NUMBER
);

CREATE TABLE orders (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    order_date DATE
);

CREATE TABLE products (
    product_id NUMBER PRIMARY KEY,
    product_name VARCHAR2(100),
    price NUMBER
);

BEGIN
    EXECUTE IMMEDIATE 'ALTER SESSION SET CURRENT_SCHEMA = c##prod_schema1';
END;

DELETE FROM customers;

CREATE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    orders_id NUMBER,
    name VARCHAR2(100)
);

CREATE TABLE orders (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    order_date TIMESTAMP
);

ALTER TABLE orders ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers(customer_id);
ALTER TABLE customers ADD CONSTRAINT fk_customers_orders FOREIGN KEY (orders_id) REFERENCES orders(order_id);

BEGIN
    compare_schemas('C##DEV_SCHEMA1', 'C##PROD_SCHEMA1');
END;
/

ALTER TABLE customers ADD CONSTRAINT fk_cust_orders
    FOREIGN KEY (customer_id) REFERENCES orders(order_id);

ALTER SESSION SET CURRENT_SCHEMA = C##DEV_SCHEMA1;
ALTER SESSION SET CURRENT_SCHEMA = C##PROD_SCHEMA1;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE orders CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP TABLE customers CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP TABLE products CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;


CREATE PROCEDURE test1
IS
BEGIN
    SELECT * from orders;
end;


CREATE OR REPLACE PROCEDURE test1
IS
BEGIN
    SELECT * from products;
end;


ALTER SESSION SET CURRENT_SCHEMA = C##DEV_SCHEMA1;


CREATE TABLE T1 (
    id NUMBER PRIMARY KEY,
    val NUMBER,
    t3_id NUMBER
);

CREATE TABLE T2 (
    id NUMBER PRIMARY KEY,
    val NUMBER,
    t1_id NUMBER
);

CREATE TABLE T3 (
    id NUMBER PRIMARY KEY,
    val NUMBER,
    t2_id NUMBER
);

ALTER TABLE T1 ADD CONSTRAINT fk_t1_t3 FOREIGN KEY (t3_id) REFERENCES t3(id);
ALTER TABLE T3 ADD CONSTRAINT fk_t3_t2 FOREIGN KEY (t2_id) REFERENCES t2(id);
ALTER TABLE T2 ADD CONSTRAINT fk_t2_t1 FOREIGN KEY (t1_id) REFERENCES t1(id);


drop table t1;
drop table t2;
drop table t3;

