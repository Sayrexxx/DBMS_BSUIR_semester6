CREATE TABLE MyTable (
    id NUMBER,
    val NUMBER
);


DECLARE
    random_val NUMBER;
BEGIN
    FOR i IN 1..10000 LOOP
        random_val := TRUNC(DBMS_RANDOM.VALUE(1, 100001));  -- TRUNC округляет до целого числа
        INSERT INTO MyTable (id, val) VALUES (i, random_val);
    END LOOP;
    COMMIT;
END;


CREATE OR REPLACE FUNCTION CheckEvenOdd RETURN VARCHAR2 IS
    even_count NUMBER := 0;
    odd_count NUMBER := 0;
BEGIN
    SELECT COUNT(*) INTO even_count FROM MyTable WHERE MOD(val, 2) = 0;
    SELECT COUNT(*) INTO odd_count FROM MyTable WHERE MOD(val, 2) != 0;

    IF even_count > odd_count THEN
        RETURN 'TRUE';
    ELSIF even_count < odd_count THEN
        RETURN 'FALSE';
    ELSE
        RETURN 'EQUAL';
    END IF;
END;

SELECT CheckEvenOdd AS result FROM dual;




CREATE OR REPLACE FUNCTION GenerateInsertCommand(p_id NUMBER) RETURN VARCHAR2 IS
    v_id NUMBER;
    v_val NUMBER;
    insert_command VARCHAR2(200);
BEGIN
    IF p_id IS NULL OR p_id < 0 THEN
        RETURN 'Ошибка: ID не может быть NULL или отрицательным.';
    END IF;

    BEGIN
        SELECT id, val INTO v_id, v_val FROM MyTable WHERE id = p_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 'Ошибка: Запись с ID ' || p_id || ' не найдена.';
    END;

    insert_command := 'INSERT INTO MyTable (id, val) VALUES (' || v_id || ', ' || v_val || ');';
    RETURN insert_command;
END;


SELECT GenerateInsertCommand(10) FROM dual;



CREATE OR REPLACE PROCEDURE InsertIntoMyTable(p_val NUMBER) IS
    v_new_id NUMBER;
BEGIN
    IF p_val IS NULL THEN
        RAISE_APPLICATION_ERROR(-20001, 'Ошибка: VAL не может быть NULL.');
    END IF;

    SELECT NVL(MAX(id), 0) + 1 INTO v_new_id FROM MyTable;

    INSERT INTO MyTable (id, val) VALUES (v_new_id, p_val);
    COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE UpdateMyTable(p_id NUMBER, p_val NUMBER) IS
    v_count NUMBER;
BEGIN
    IF p_id IS NULL OR p_val IS NULL OR p_id < 0 OR p_val < 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Ошибка: ID и VAL не могут быть NULL или отрицательными.');
    END IF;

    SELECT COUNT(*) INTO v_count FROM MyTable WHERE id = p_id;
    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20003, 'Ошибка: Запись с ID ' || p_id || ' не найдена.');
    END IF;

    UPDATE MyTable SET val = p_val WHERE id = p_id;
    COMMIT;
END;


CREATE OR REPLACE PROCEDURE DeleteFromMyTable(p_id NUMBER) IS
    v_count NUMBER;
BEGIN
    IF p_id IS NULL OR p_id < 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Ошибка: ID не может быть NULL или отрицательным.');
    END IF;

    SELECT COUNT(*) INTO v_count FROM MyTable WHERE id = p_id;
    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20003, 'Ошибка: Запись с ID ' || p_id || ' не найдена.');
    END IF;

    DELETE FROM MyTable WHERE id = p_id;
    COMMIT;
END;


BEGIN
    InsertIntoMyTable(128);
    InsertIntoMyTable(593);
    UpdateMyTable(10001, 12983892193);
    DeleteFromMyTable(10002);
    DeleteFromMyTable(10003);
END;




CREATE OR REPLACE FUNCTION CalculateAnnualBonus(
    p_monthly_salary NUMBER,
    p_bonus_percent NUMBER
) RETURN NUMBER IS
    annual_bonus NUMBER;
BEGIN
    IF p_monthly_salary IS NULL OR p_bonus_percent IS NULL THEN
        RAISE_APPLICATION_ERROR(-20001, 'Ошибка: Месячная зарплата и процент премиальных не могут быть NULL.');
    END IF;

    IF p_monthly_salary < 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Ошибка: Месячная зарплата не может быть отрицательной.');
    END IF;

    IF p_bonus_percent < 0 THEN
        RAISE_APPLICATION_ERROR(-20003, 'Ошибка: Процент премиальных не может быть отрицательным.');
    END IF;

    IF p_bonus_percent > 100 THEN
        RAISE_APPLICATION_ERROR(-20004, 'Ошибка: Процент премиальных не может быть больше 100.');
    END IF;

    IF p_bonus_percent != TRUNC(p_bonus_percent) THEN
        RAISE_APPLICATION_ERROR(-20004, 'Ошибка: Процент премиальных должен быть целым.');
    END IF;

    annual_bonus := (1 + p_bonus_percent / 100) * 12 * p_monthly_salary;

    RETURN annual_bonus;
END;


SELECT CalculateAnnualBonus(5000, 10) FROM dual;


-- SELECT
--   ORIGINAL_NAME,
--   DROPTIME
-- FROM
--   user_recyclebin
-- WHERE
--   OPERATION = 'DROP'
--   and TYPE = 'TABLE'
-- order by
--   DROPTIME desc;
--
--
-- flashback table MyTable to before drop rename to MyTable;