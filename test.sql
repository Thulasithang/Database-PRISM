-- Test SELECT with function call
SELECT name, double_price(price) FROM users;

-- Test WHERE with boolean function
SELECT name, price FROM users WHERE is_expensive(price);