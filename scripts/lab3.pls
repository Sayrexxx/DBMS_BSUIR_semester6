CREATE OR REPLACE PROCEDURE compare_schemas(
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2
)   AUTHID CURRENT_USER
IS
    v_diff_count NUMBER;
    TYPE t_varchar_table IS TABLE OF VARCHAR2(128);
    v_dev_ddl        CLOB;
    v_prod_ddl       CLOB;

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

    DBMS_OUTPUT.PUT_LINE('Step 8: Scripts to fix differences in schemas:');

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
