CREATE FUNCTION price_div_two(price INT) RETURNS INT
BEGIN
    RETURN price / 2;
END;

-- CREATE FUNCTION is_expensive(price INT) RETURNS BOOL
-- BEGIN
--     IF price > 100 THEN
--         RETURN true;
--     ELSE
--         RETURN false;
--     END IF;
-- END;

-- Test SELECT with function call
SELECT name, price, price_div_two(price) FROM users;

-- Test WHERE with boolean function
-- SELECT name, price FROM users WHERE is_expensive(price);