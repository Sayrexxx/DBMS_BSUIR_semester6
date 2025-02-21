CREATE TABLE MyTable (
    id NUMBER,
    val NUMBER
);


DECLARE
    random_val NUMBER;
BEGIN
    FOR i IN 1..10000 LOOP
        -- Генерация случайного целого числа от 1 до 100000
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


