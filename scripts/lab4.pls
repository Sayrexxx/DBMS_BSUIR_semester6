CREATE OR REPLACE PROCEDURE dynamic_sql_executor(
    p_json IN CLOB,
    p_cursor OUT SYS_REFCURSOR,
    p_rows OUT NUMBER,
    p_message OUT VARCHAR2
)
AS
    v_json_obj            JSON_OBJECT_T;
    v_query_type          VARCHAR2(50);
    v_select_columns      VARCHAR2(32767);
    v_tables              VARCHAR2(32767);
    v_join_conditions     VARCHAR2(32767);
    v_where_conditions    VARCHAR2(32767);
    v_subquery_conditions VARCHAR2(32767);
    v_group_by            VARCHAR2(32767);
    v_filter_clause       VARCHAR2(32767);
    v_query               VARCHAR2(32767);
    v_table               VARCHAR2(100);
    v_columns             VARCHAR2(32767);
    v_values              VARCHAR2(32767);
    v_set_clause          VARCHAR2(32767);
    v_ddl_command         VARCHAR2(50);
    v_fields              VARCHAR2(32767);
    v_generate_trigger    VARCHAR2(5);
    v_trigger_name        VARCHAR2(100);
    v_pk_field            VARCHAR2(100);
    v_sequence_name       VARCHAR2(100);
    v_trigger_sql         VARCHAR2(32767);

BEGIN
    v_json_obj := JSON_OBJECT_T.parse(p_json);
    v_query_type := UPPER(v_json_obj.get_String('query_type'));

    IF v_query_type = 'SELECT' THEN
        v_select_columns := v_json_obj.get_String('select_columns');
        v_tables := v_json_obj.get_String('tables');
        BEGIN
            v_join_conditions := v_json_obj.get_String('join_conditions');
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_join_conditions := NULL;
        END;
        BEGIN
            v_where_conditions := v_json_obj.get_String('where_conditions');
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_where_conditions := NULL;
        END;
        BEGIN
            v_subquery_conditions := v_json_obj.get_String('subquery_conditions');
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_subquery_conditions := NULL;
        END;
        BEGIN
            v_group_by := v_json_obj.get_String('group_by');
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_group_by := NULL;
        END;

        v_filter_clause := NULL;
        IF v_join_conditions IS NOT NULL AND TRIM(v_join_conditions) IS NOT NULL THEN
            v_filter_clause := v_join_conditions;
        END IF;
        IF v_where_conditions IS NOT NULL AND TRIM(v_where_conditions) IS NOT NULL THEN
            IF v_filter_clause IS NOT NULL THEN
                v_filter_clause := v_filter_clause || ' AND ' || v_where_conditions;
            ELSE
                v_filter_clause := v_where_conditions;
            END IF;
        END IF;
        IF v_subquery_conditions IS NOT NULL AND TRIM(v_subquery_conditions) IS NOT NULL THEN
            IF v_filter_clause IS NOT NULL THEN
                v_filter_clause := v_filter_clause || ' AND ' || v_subquery_conditions;
            ELSE
                v_filter_clause := v_subquery_conditions;
            END IF;
        END IF;

        v_query := 'SELECT ' || v_select_columns || ' FROM ' || v_tables;
        IF v_filter_clause IS NOT NULL AND TRIM(v_filter_clause) IS NOT NULL THEN
            v_query := v_query || ' WHERE ' || v_filter_clause;
        END IF;
        IF v_group_by IS NOT NULL AND TRIM(v_group_by) IS NOT NULL THEN
            v_query := v_query || ' GROUP BY ' || v_group_by;
        END IF;

        p_message := 'Выполняется SELECT запрос.';
        p_rows := 0;
        OPEN p_cursor FOR v_query;

    ELSIF v_query_type IN ('INSERT', 'UPDATE', 'DELETE') THEN
        v_table := v_json_obj.get_String('table');
        IF v_query_type = 'INSERT' THEN
            v_columns := v_json_obj.get_String('columns');
            v_values := v_json_obj.get_String('values');
            v_query := 'INSERT INTO ' || v_table || ' (' || v_columns || ') VALUES (' || v_values || ')';

        ELSIF v_query_type = 'UPDATE' THEN
            v_set_clause := v_json_obj.get_String('set_clause');
            BEGIN
                v_where_conditions := v_json_obj.get_String('where_conditions');
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_where_conditions := NULL;
            END;
            BEGIN
                v_subquery_conditions := v_json_obj.get_String('subquery_conditions');
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_subquery_conditions := NULL;
            END;

            v_filter_clause := NULL;
            IF v_where_conditions IS NOT NULL AND TRIM(v_where_conditions) IS NOT NULL THEN
                v_filter_clause := v_where_conditions;
            END IF;
            IF v_subquery_conditions IS NOT NULL AND TRIM(v_subquery_conditions) IS NOT NULL THEN
                IF v_filter_clause IS NOT NULL THEN
                    v_filter_clause := v_filter_clause || ' AND ' || v_subquery_conditions;
                ELSE
                    v_filter_clause := v_subquery_conditions;
                END IF;
            END IF;

            v_query := 'UPDATE ' || v_table || ' SET ' || v_set_clause;
            IF v_filter_clause IS NOT NULL AND TRIM(v_filter_clause) IS NOT NULL THEN
                v_query := v_query || ' WHERE ' || v_filter_clause;
            END IF;

        ELSIF v_query_type = 'DELETE' THEN
            BEGIN
                v_where_conditions := v_json_obj.get_String('where_conditions');
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_where_conditions := NULL;
            END;
            BEGIN
                v_subquery_conditions := v_json_obj.get_String('subquery_conditions');
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_subquery_conditions := NULL;
            END;

            v_filter_clause := NULL;
            IF v_where_conditions IS NOT NULL AND TRIM(v_where_conditions) IS NOT NULL THEN
                v_filter_clause := v_where_conditions;
            END IF;
            IF v_subquery_conditions IS NOT NULL AND TRIM(v_subquery_conditions) IS NOT NULL THEN
                IF v_filter_clause IS NOT NULL THEN
                    v_filter_clause := v_filter_clause || ' AND ' || v_subquery_conditions;
                ELSE
                    v_filter_clause := v_subquery_conditions;
                END IF;
            END IF;

            v_query := 'DELETE FROM ' || v_table;
            IF v_filter_clause IS NOT NULL AND TRIM(v_filter_clause) IS NOT NULL THEN
                v_query := v_query || ' WHERE ' || v_filter_clause;
            END IF;
        END IF;

        p_message := 'DML операция ' || v_query_type || ' выполнена.';
        p_cursor := NULL;
        EXECUTE IMMEDIATE v_query;
        p_rows := SQL%ROWCOUNT;

    ELSIF v_query_type = 'DDL' THEN
        v_ddl_command := UPPER(v_json_obj.get_String('ddl_command'));
        v_table := v_json_obj.get_String('table');

        IF v_ddl_command = 'CREATE TABLE' THEN
            v_fields := v_json_obj.get_String('fields');
            v_query := 'CREATE TABLE ' || v_table || ' (' || v_fields || ')';
            EXECUTE IMMEDIATE v_query;
            p_message := 'Таблица ' || v_table || ' создана.';
            p_rows := 0;
            p_cursor := NULL;

            BEGIN
                v_generate_trigger := v_json_obj.get_String('generate_trigger');
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_generate_trigger := 'false';
            END;

            IF LOWER(v_generate_trigger) = 'true' THEN
                v_trigger_name := v_json_obj.get_String('trigger_name');
                v_pk_field := v_json_obj.get_String('pk_field');
                v_sequence_name := v_json_obj.get_String('sequence_name');

                BEGIN
                    EXECUTE IMMEDIATE 'CREATE SEQUENCE ' || v_sequence_name;
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                END;

                v_trigger_sql :=
                        'CREATE OR REPLACE TRIGGER ' || v_trigger_name || ' ' ||
                        'BEFORE INSERT ON ' || v_table || ' ' ||
                        'FOR EACH ROW ' ||
                        'WHEN (new.' || v_pk_field || ' IS NULL) ' ||
                        'BEGIN ' ||
                        '  SELECT ' || v_sequence_name || '.NEXTVAL INTO :new.' || v_pk_field || ' FROM dual; ' ||
                        'END;';
                EXECUTE IMMEDIATE v_trigger_sql;
                p_message := p_message || ' Триггер ' || v_trigger_name || ' создан.';
            END IF;

        ELSIF v_ddl_command = 'DROP TABLE' THEN
            v_query := 'DROP TABLE ' || v_table;
            EXECUTE IMMEDIATE v_query;
            p_message := 'Таблица ' || v_table || ' удалена.';
            p_rows := 0;
            p_cursor := NULL;
        ELSE
            RAISE_APPLICATION_ERROR(-20001, 'Не поддерживаемая DDL команда: ' || v_ddl_command);
        END IF;

    ELSE
        RAISE_APPLICATION_ERROR(-20001, 'Не поддерживаемый тип запроса: ' || v_query_type);
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        p_message := 'Ошибка: ' || SQLERRM;
        p_rows := 0;
        p_cursor := NULL;
END dynamic_sql_executor;
/



-- SELECT
DECLARE
    v_cursor   SYS_REFCURSOR;
    v_rows     NUMBER;
    v_message  VARCHAR2(4000);
    v_json     CLOB;
    v_s_id     NUMBER;
    v_s_name   VARCHAR2(100);
    v_group_id NUMBER;
    v_g_name   VARCHAR2(100);
BEGIN
    v_json := '{
    "query_type": "SELECT",
    "select_columns": "s.id, s.name, s.group_id, g.name",
    "tables": "students s, groups g",
    "join_conditions": "s.group_id = g.id",
    "where_conditions": "g.c_val > 1",
    "group_by": "s.id, s.name, s.group_id, g.name"
  }';

    dynamic_sql_executor(v_json, v_cursor, v_rows, v_message);
    DBMS_OUTPUT.PUT_LINE(v_message);
    LOOP
        FETCH v_cursor INTO v_s_id, v_s_name, v_group_id, v_g_name;
        EXIT WHEN v_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('Student ID: ' || v_s_id || ', Student name: ' || v_s_name ||
                             ', Group ID: ' || v_group_id || ', Group name: ' || v_g_name);
    END LOOP;
    CLOSE v_cursor;
END;
/


-- INSERT
DECLARE
  v_cursor SYS_REFCURSOR;
  v_rows NUMBER;
  v_message VARCHAR2(4000);
  v_json CLOB;
BEGIN
  v_json := '{
    "query_type": "INSERT",
    "table": "groups",
    "columns": "name",
    "values": "''тестовое название группы для вставки''"
  }';

  dynamic_sql_executor(v_json, v_cursor, v_rows, v_message);
  DBMS_OUTPUT.PUT_LINE(v_message || ', затронуто строк: ' || v_rows);
END;

DECLARE
  v_cursor SYS_REFCURSOR;
  v_rows NUMBER;
  v_message VARCHAR2(4000);
  v_json CLOB;
BEGIN
  v_json := '{
    "query_type": "INSERT",
    "table": "students",
    "columns": "id, name, group_id",
    "values": "20, ''тестовый студент'', 1"
  }';

  dynamic_sql_executor(v_json, v_cursor, v_rows, v_message);
  DBMS_OUTPUT.PUT_LINE(v_message || ', затронуто строк: ' || v_rows);
END;
/


-- UPDATE
DECLARE
  v_cursor SYS_REFCURSOR;
  v_rows NUMBER;
  v_message VARCHAR2(4000);
  v_json CLOB;
BEGIN
  v_json := '{
    "query_type": "UPDATE",
    "table": "students",
    "set_clause": "name = ''изменённое тестовое имя для студента''",
    "where_conditions": "id = 20",
    "subquery_conditions": "group_id = (SELECT id FROM groups WHERE id = 1)"
  }';

  dynamic_sql_executor(v_json, v_cursor, v_rows, v_message);
  DBMS_OUTPUT.PUT_LINE(v_message || ', затронуто строк: ' || v_rows);
END;
/


-- DELETE
DECLARE
  v_cursor SYS_REFCURSOR;
  v_rows NUMBER;
  v_message VARCHAR2(4000);
  v_json CLOB;
BEGIN
  v_json := '{
    "query_type": "DELETE",
    "table": "students",
    "where_conditions": "id = 20"
  }';

  dynamic_sql_executor(v_json, v_cursor, v_rows, v_message);
  DBMS_OUTPUT.PUT_LINE(v_message || ', затронуто строк: ' || v_rows);
END;
/

GRANT CREATE TABLE TO SYSTEM;
GRANT CREATE SEQUENCE TO SYSTEM;
GRANT CREATE TRIGGER TO SYSTEM;


-- CREATE TABLE (with trigger)

DECLARE
  v_json_input CLOB := '{
    "query_type": "DDL",
    "ddl_command": "CREATE TABLE",
    "table": "test_table_with_trigger",
    "fields": "id NUMBER, name VARCHAR2(50)",
    "generate_trigger": "true",
    "trigger_name": "test_table_trigger",
    "pk_field": "id",
    "sequence_name": "test_table_seq"
  }';

  v_cursor  SYS_REFCURSOR;
  v_rows    NUMBER;
  v_message VARCHAR2(4000);

BEGIN
  dynamic_sql_executor(v_json_input, v_cursor, v_rows, v_message);

  DBMS_OUTPUT.PUT_LINE('Сообщение: ' || v_message);
END;
/

DECLARE
  v_json_input CLOB := '{
    "query_type": "INSERT",
    "table": "test_table_with_trigger",
    "columns": "name",
    "values": "''Test Record''"
  }';

  v_cursor  SYS_REFCURSOR;
  v_rows    NUMBER;
  v_message VARCHAR2(4000);

BEGIN
  dynamic_sql_executor(
    p_json    => v_json_input,
    p_cursor  => v_cursor,
    p_rows    => v_rows,
    p_message => v_message
  );

  DBMS_OUTPUT.PUT_LINE('Сообщение: ' || v_message);
  DBMS_OUTPUT.PUT_LINE('Затронуто строк: ' || v_rows);
END;
/


-- DROP TABLE
DECLARE
  v_json_input CLOB := '{
    "query_type": "DDL",
    "ddl_command": "DROP TABLE",
    "table": "test_table_with_trigger"
  }';

  v_cursor  SYS_REFCURSOR;
  v_rows    NUMBER;
  v_message VARCHAR2(4000);

BEGIN
  dynamic_sql_executor(v_json_input, v_cursor, v_rows, v_message);

  DBMS_OUTPUT.PUT_LINE('Сообщение: ' || v_message);
END;
/

SELECT * FROM test_table_with_trigger;