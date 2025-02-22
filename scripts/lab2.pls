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

    IF INSERTING THEN
        SELECT COUNT(*) INTO v_count FROM STUDENTS WHERE ID = :NEW.ID;
        IF v_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20002, 'Ошибка: Студент с ID ' || :NEW.ID || ' уже существует.');
        END IF;
    END IF;
END;


CREATE OR REPLACE TRIGGER trg_check_group_name
BEFORE INSERT OR UPDATE ON GROUPS
FOR EACH ROW
DECLARE
    v_count NUMBER;
BEGIN
    IF :NEW.NAME IS NULL THEN
        RAISE_APPLICATION_ERROR(-20003, 'Ошибка: Название группы не может быть NULL.');
    END IF;

    SELECT COUNT(*) INTO v_count FROM GROUPS WHERE NAME = :NEW.NAME;
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20004, 'Ошибка: Группа с названием ' || :NEW.NAME || ' уже существует.');
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
    ACTION_DATE DATE,
    OLD_NAME VARCHAR2(100),
    NEW_NAME VARCHAR2(100),
    OLD_GROUP_ID NUMBER,
    NEW_GROUP_ID NUMBER
);

CREATE OR REPLACE TRIGGER trg_students_audit
AFTER INSERT OR UPDATE OR DELETE ON STUDENTS
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        INSERT INTO STUDENTS_AUDIT (ACTION, STUDENT_ID, ACTION_DATE, NEW_NAME, NEW_GROUP_ID)
        VALUES ('INSERT', :NEW.ID, SYSDATE, :NEW.NAME, :NEW.GROUP_ID);
    ELSIF UPDATING THEN
        INSERT INTO STUDENTS_AUDIT (ACTION, STUDENT_ID, ACTION_DATE, OLD_NAME, NEW_NAME, OLD_GROUP_ID, NEW_GROUP_ID)
        VALUES ('UPDATE', :OLD.ID, SYSDATE, :OLD.NAME, :NEW.NAME, :OLD.GROUP_ID, :NEW.GROUP_ID);
    ELSIF DELETING THEN
        INSERT INTO STUDENTS_AUDIT (ACTION, STUDENT_ID, ACTION_DATE, OLD_NAME, OLD_GROUP_ID)
        VALUES ('DELETE', :OLD.ID, SYSDATE, :OLD.NAME, :OLD.GROUP_ID);
    END IF;
END;


-- Task 5
CREATE OR REPLACE PROCEDURE RestoreStudents(p_restore_date DATE) IS
BEGIN
    DELETE FROM STUDENTS;

    FOR rec IN (SELECT * FROM STUDENTS_AUDIT WHERE ACTION_DATE <= p_restore_date ORDER BY ACTION_DATE) LOOP
        IF rec.ACTION = 'INSERT' THEN
            INSERT INTO STUDENTS (ID, NAME, GROUP_ID) VALUES (rec.STUDENT_ID, rec.NEW_NAME, rec.NEW_GROUP_ID);
        ELSIF rec.ACTION = 'UPDATE' THEN
            UPDATE STUDENTS SET NAME = rec.NEW_NAME, GROUP_ID = rec.NEW_GROUP_ID WHERE ID = rec.STUDENT_ID;
        ELSIF rec.ACTION = 'DELETE' THEN
            DELETE FROM STUDENTS WHERE ID = rec.STUDENT_ID;
        END IF;
    END LOOP;

    COMMIT;
END;

BEGIN
    RestoreStudents(DATE())
end;


-- Task 6
CREATE OR REPLACE TRIGGER trg_update_group_count
AFTER INSERT OR UPDATE OR DELETE ON STUDENTS
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        UPDATE GROUPS SET C_VAL = C_VAL + 1 WHERE ID = :NEW.GROUP_ID;
    ELSIF UPDATING THEN
        UPDATE GROUPS SET C_VAL = C_VAL - 1 WHERE ID = :OLD.GROUP_ID;
        UPDATE GROUPS SET C_VAL = C_VAL + 1 WHERE ID = :NEW.GROUP_ID;
    ELSIF DELETING THEN
        UPDATE GROUPS SET C_VAL = C_VAL - 1 WHERE ID = :OLD.GROUP_ID;
    END IF;
END;
/