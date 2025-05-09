-- Task 1

CREATE TABLE GROUPS (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR2(100) NOT NULL,
    C_VAL NUMBER DEFAULT 0
);

CREATE TABLE STUDENTS (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR2(100) NOT NULL,
    GROUP_ID NUMBER,
    CONSTRAINT fk_group FOREIGN KEY (GROUP_ID) REFERENCES GROUPS(ID) ON DELETE CASCADE
);


-- Task 2
CREATE OR REPLACE TRIGGER trg_check_student_id
BEFORE INSERT OR UPDATE ON STUDENTS
FOR EACH ROW
DECLARE
    v_count NUMBER;
BEGIN
    IF :NEW.ID IS NULL OR :NEW.ID < 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Ошибка: ID студента не может быть NULL или отрицательным.');
    END IF;

    IF INSERTING OR UPDATING THEN
        SELECT COUNT(*) INTO v_count FROM STUDENTS WHERE ID = :NEW.ID;
        IF v_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20002, 'Ошибка: Студент с ID ' || :NEW.ID || ' уже существует.');
        END IF;
    END IF;
END;


CREATE OR REPLACE TRIGGER trg_check_group_name
BEFORE INSERT ON GROUPS
FOR EACH ROW
DECLARE
    v_count NUMBER;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    IF :NEW.NAME IS NULL THEN
        RAISE_APPLICATION_ERROR(-20003, 'Ошибка: Название группы не может быть NULL.');
    END IF;

    IF INSERTING OR UPDATING THEN
        SELECT COUNT(*) INTO v_count FROM GROUPS WHERE NAME = :NEW.NAME AND ID != :NEW.ID;
        IF v_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20004, 'Ошибка: Группа с названием ' || :NEW.NAME || ' уже существует.');
        END IF;
    END IF;
END;


CREATE OR REPLACE TRIGGER trg_generate_group_id
BEFORE INSERT ON GROUPS
FOR EACH ROW
BEGIN
    IF :NEW.ID IS NULL THEN
        SELECT NVL(MAX(ID), 0) + 1 INTO :NEW.ID FROM GROUPS;
    END IF;
END;


CREATE OR REPLACE TRIGGER trg_cascade_delete_students
AFTER DELETE ON GROUPS
FOR EACH ROW
BEGIN
    DELETE FROM STUDENTS WHERE GROUP_ID = :OLD.ID;
END;

SELECT * FROM USER_TRIGGERS;


-- Task  4
CREATE TABLE STUDENTS_AUDIT (
    ACTION VARCHAR2(10),
    STUDENT_ID NUMBER,
    ACTION_TIMESTAMP TIMESTAMP,
    OLD_NAME VARCHAR2(100),
    NEW_NAME VARCHAR2(100),
    OLD_GROUP_ID NUMBER,
    NEW_GROUP_ID NUMBER
);

CREATE OR REPLACE TRIGGER trg_groups_audit
AFTER INSERT OR UPDATE OR DELETE ON GROUPS
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        INSERT INTO GROUPS_AUDIT (ACTION, GROUP_ID, ACTION_TIMESTAMP, NEW_NAME, NEW_C_VAL)
        VALUES ('INSERT', :NEW.ID, SYSTIMESTAMP, :NEW.NAME, :NEW.C_VAL);
    ELSIF UPDATING THEN
        INSERT INTO GROUPS_AUDIT (ACTION, GROUP_ID, ACTION_TIMESTAMP, OLD_NAME, NEW_NAME, OLD_C_VAL, NEW_C_VAL)
        VALUES ('UPDATE', :OLD.ID, SYSTIMESTAMP, :OLD.NAME, :NEW.NAME, :OLD.C_VAL, :NEW.C_VAL);
    ELSIF DELETING THEN
        INSERT INTO GROUPS_AUDIT (ACTION, GROUP_ID, ACTION_TIMESTAMP, OLD_NAME, OLD_C_VAL)
        VALUES ('DELETE', :OLD.ID, SYSTIMESTAMP, :OLD.NAME, :OLD.C_VAL);
    END IF;
END;

CREATE OR REPLACE TRIGGER trg_students_audit
AFTER INSERT OR UPDATE OR DELETE ON STUDENTS
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        INSERT INTO STUDENTS_AUDIT (ACTION, STUDENT_ID, ACTION_TIMESTAMP, NEW_NAME, NEW_GROUP_ID)
        VALUES ('INSERT', :NEW.ID, SYSTIMESTAMP, :NEW.NAME, :NEW.GROUP_ID);
    ELSIF UPDATING THEN
        INSERT INTO STUDENTS_AUDIT (ACTION, STUDENT_ID, ACTION_TIMESTAMP, OLD_NAME, NEW_NAME, OLD_GROUP_ID, NEW_GROUP_ID)
        VALUES ('UPDATE', :OLD.ID, SYSTIMESTAMP, :OLD.NAME, :NEW.NAME, :OLD.GROUP_ID, :NEW.GROUP_ID);
    ELSIF DELETING THEN
        INSERT INTO STUDENTS_AUDIT (ACTION, STUDENT_ID, ACTION_TIMESTAMP, OLD_NAME, OLD_GROUP_ID)
        VALUES ('DELETE', :OLD.ID, SYSTIMESTAMP, :OLD.NAME, :OLD.GROUP_ID);
    END IF;
END;


-- Task 5

CREATE TABLE STUDENTS_BACKUP (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR2(100) NOT NULL,
    GROUP_ID NUMBER
);

CREATE OR REPLACE PROCEDURE RestoreStudents(p_restore_date TIMESTAMP) IS
BEGIN
    -- Отключаем триггеры, которые могут мешать восстановлению
    EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_CHECK_STUDENT_ID DISABLE';
    EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_CHECK_GROUP_NAME DISABLE';
    EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_UPDATE_GROUP_COUNT DISABLE';

    -- Очистка таблиц STUDENTS и GROUPS
    DELETE FROM STUDENTS;
    DELETE FROM GROUPS;

    -- Восстановление групп из GROUPS_AUDIT
    FOR rec IN (
        SELECT *
        FROM GROUPS_AUDIT
        WHERE ACTION_TIMESTAMP <= p_restore_date
        ORDER BY ACTION_TIMESTAMP
    ) LOOP
        IF rec.ACTION = 'INSERT' THEN
            -- Вставка новой группы
            BEGIN
                INSERT INTO GROUPS (ID, NAME, C_VAL)
                VALUES (rec.GROUP_ID, rec.NEW_NAME, rec.NEW_C_VAL);
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    -- Если группа уже существует, обновляем её
                    UPDATE GROUPS
                    SET NAME = rec.NEW_NAME, C_VAL = rec.NEW_C_VAL
                    WHERE ID = rec.GROUP_ID;
            END;
        ELSIF rec.ACTION = 'UPDATE' THEN
            -- Обновление существующей группы
            UPDATE GROUPS
            SET NAME = rec.NEW_NAME, C_VAL = rec.NEW_C_VAL
            WHERE ID = rec.GROUP_ID;
        ELSIF rec.ACTION = 'DELETE' THEN
            -- Удаление группы
            DELETE FROM GROUPS WHERE ID = rec.GROUP_ID;
        END IF;
    END LOOP;

    -- Восстановление студентов из STUDENTS_AUDIT
    FOR rec IN (
        SELECT *
        FROM STUDENTS_AUDIT
        WHERE ACTION_TIMESTAMP <= p_restore_date
        ORDER BY ACTION_TIMESTAMP
    ) LOOP
        IF rec.ACTION = 'INSERT' THEN
            -- Вставка нового студента
            BEGIN
                INSERT INTO STUDENTS (ID, NAME, GROUP_ID)
                VALUES (rec.STUDENT_ID, rec.NEW_NAME, rec.NEW_GROUP_ID);
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    -- Если студент уже существует, обновляем его
                    UPDATE STUDENTS
                    SET NAME = rec.NEW_NAME, GROUP_ID = rec.NEW_GROUP_ID
                    WHERE ID = rec.STUDENT_ID;
            END;
        ELSIF rec.ACTION = 'UPDATE' THEN
            -- Обновление существующего студента
            UPDATE STUDENTS
            SET NAME = rec.NEW_NAME, GROUP_ID = rec.NEW_GROUP_ID
            WHERE ID = rec.STUDENT_ID;
        ELSIF rec.ACTION = 'DELETE' THEN
            -- Удаление студента
            DELETE FROM STUDENTS WHERE ID = rec.STUDENT_ID;
        END IF;
    END LOOP;

    -- Включаем триггеры обратно
    EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_CHECK_STUDENT_ID ENABLE';
    EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_CHECK_GROUP_NAME ENABLE';
    EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_UPDATE_GROUP_COUNT ENABLE';

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        -- В случае ошибки включаем триггеры и откатываем изменения
        EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_CHECK_STUDENT_ID ENABLE';
        EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_CHECK_GROUP_NAME ENABLE';
        EXECUTE IMMEDIATE 'ALTER TRIGGER TRG_UPDATE_GROUP_COUNT ENABLE';
        ROLLBACK;
        RAISE; -- Повторно выбрасываем исключение
END;

BEGIN
    RestoreStudents(TIMESTAMP '2025-03-03 16:37:16.146811');
end;


-- Task 6
CREATE OR REPLACE TRIGGER trg_update_group_count
AFTER INSERT OR DELETE ON STUDENTS
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        UPDATE GROUPS SET C_VAL = C_VAL + 1 WHERE ID = :NEW.GROUP_ID;
    ELSIF DELETING THEN
        BEGIN
            UPDATE GROUPS SET C_VAL = C_VAL - 1 WHERE ID = :OLD.GROUP_ID;
        EXCEPTION
            WHEN OTHERS THEN
                NULL;
        END;
    END IF;
END;

CREATE TABLE GROUPS_AUDIT (
    ACTION VARCHAR2(10),
    GROUP_ID NUMBER,
    ACTION_TIMESTAMP TIMESTAMP,
    OLD_NAME VARCHAR2(100),
    NEW_NAME VARCHAR2(100),
    OLD_C_VAL NUMBER,
    NEW_C_VAL NUMBER
);


SELECT * FROM USER_TRIGGERS;

INSERT INTO GROUPS (NAME) VALUES ('русские');
INSERT INTO GROUPS (NAME) VALUES ('нерусские');

INSERT INTO STUDENTS (ID, NAME, GROUP_ID) VALUES (5, 'женя', 1);
INSERT INTO STUDENTS (ID, NAME, GROUP_ID) VALUES (6, 'женя1', 1);
INSERT INTO STUDENTS (ID, NAME, GROUP_ID) VALUES (10, 'махачкала', 2);

SELECT * FROM GROUPS;
SELECT * FROM STUDENTS;
SELECT * FROM STUDENTS_AUDIT;
