
-- Create is_adult function with if-else
CREATE FUNCTION is_adult(age INT) RETURNS TEXT
BEGIN
    IF age >= 18 THEN
        RETURN 'adult';
    ELSE
        RETURN 'child';
    END IF;
END;

-- Test the function
SELECT name, age, is_adult(age) FROM users; 