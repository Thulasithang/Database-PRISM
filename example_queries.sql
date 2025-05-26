
CREATE FUNCTION is_adult(age INT) RETURNS INT
BEGIN
    IF age >= 18 THEN
        RETURN 'adult';
    ELSE
        RETURN 'child';
    END IF;
END;


-- Query data using functions
SELECT name, is_adult(price) FROM users;
SELECT name, price FROM users WHERE is_expensive(price); 