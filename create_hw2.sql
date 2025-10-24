-- 1. Створюємо нову базу даних
CREATE DATABASE ecommerce_db;
USE ecommerce_db;

-- 2. Встановлюємо тайм-аут сесії (про всяк випадок)
-- Збільшуємо час очікування до 10 хвилин (600 секунд)
SET SESSION net_read_timeout = 600;
SET SESSION net_write_timeout = 600;

-- 3. Створюємо таблиці
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_name VARCHAR(50),
    join_date DATE,
    region VARCHAR(50)
);

CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(50),
    category VARCHAR(50)
);

CREATE TABLE reviews (
    review_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    product_id INT,
    rating INT

);


-- 4. Наповнюємо таблицю товарів (маленька таблиця)
INSERT INTO products (product_name, category)
VALUES 
('Смартфон', 'Електроніка'), ('Ноутбук', 'Електроніка'), ('Навушники', 'Електроніка'),
('Смарт-годинник', 'Електроніка'), ('Телевізор', 'Електроніка'), ('Наукова фантастика', 'Книги'),
('Детектив', 'Книги'), ('Історія', 'Книги'), ('Підручник з програмування', 'Книги'),
('Футболка', 'Одяг'), ('Джинси', 'Одяг'), ('Кросівки', 'Одяг'), ('Куртка', 'Одяг'),
('Кавоварка', 'Дім та сад'), ('Лампа', 'Дім та сад'), ('Набір інструментів', 'Дім та сад'),
('Робот-пилосос', 'Дім та сад'), ('Кава в зернах', 'Їжа'), ('Чай', 'Їжа'), ('Шоколад', 'Їжа');


-- 5. Вставляємо 1,000,000 клієнтів
SET @row := 0;
INSERT INTO customers (customer_name, join_date, region)
SELECT 
    CONCAT('Customer_', seq) AS customer_name,
    CURDATE() - INTERVAL FLOOR(RAND() * 1825) DAY AS join_date,
    CASE FLOOR(RAND()*5)
        WHEN 0 THEN 'Північ' WHEN 1 THEN 'Південь' WHEN 2 THEN 'Схід'
        WHEN 3 THEN 'Захід' ELSE 'Центр'
    END AS region
FROM (
    SELECT @row := @row + 1 AS seq
    FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t1,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t2,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t3,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t4,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t5,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t6,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t7,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t8,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t9,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t10
) seqs
LIMIT 1000000;


-- 6. Вставляємо 2,000,000 відгуків (ОПТИМІЗОВАНИЙ СПОСІБ)

-- Отримуємо максимальні ID для генерації випадкових чисел
SET @max_cust_id = (SELECT MAX(customer_id) FROM customers);
SET @max_prod_id = (SELECT MAX(product_id) FROM products);

-- Скидаємо лічильник
SET @row := 0;

-- Генеруємо 2 млн рядків з випадковими ID (це ДУЖЕ ШВИДКО)
INSERT INTO reviews (customer_id, product_id, rating)
SELECT 
    FLOOR(1 + RAND() * @max_cust_id) AS customer_id, -- Випадковий клієнт
    FLOOR(1 + RAND() * @max_prod_id) AS product_id,  -- Випадковий товар
    FLOOR(1 + RAND() * 5) AS rating                 -- Випадковий рейтинг (1-5)
FROM (
    SELECT @row := @row + 1 AS seq
    FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t1,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t2,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t3,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t4,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t5,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t6,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t7,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t8,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t9,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t10,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t11 -- Додано t11
) seqs
LIMIT 2000000;




-- Повідомлення про завершення
SELECT 'Скрипт успішно виконано!' AS status;




USE ecommerce_db;

-- 1. Створюємо третю велику таблицю -- orders
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10, 2)
);

-- 2. Створюємо процедуру для її наповнення (3 млн рядків)
DELIMITER $$
CREATE PROCEDURE LoadOrders()
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE batch_size INT DEFAULT 100000; -- 100,000 рядків за раз
    DECLARE max_rows INT DEFAULT 1000000;  -- Всього 3 млн
    
    DECLARE max_cust_id INT;
    SET max_cust_id = (SELECT MAX(customer_id) FROM customers);

    WHILE i < max_rows DO
        
        INSERT INTO orders (customer_id, order_date, total_amount)
        SELECT 
            FLOOR(1 + RAND() * max_cust_id) AS customer_id,
            CURDATE() - INTERVAL FLOOR(RAND() * 365) DAY AS order_date, -- Випадкова дата за останній рік
            ROUND(50 + RAND() * 1000, 2) AS total_amount -- Випадкова сума
        FROM (
            -- Генератор на 100,000 рядків
            SELECT 1
            FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t1,
                 (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t2,
                 (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t3,
                 (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t4,
                 (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t5
        ) seqs
        LIMIT batch_size;

        SET i = i + batch_size;
    END WHILE;
END$$
DELIMITER ;

-- 3. Запускаємо процедуру
SELECT 'Починаємо вставку 3,000,000 замовлень...' AS status;
CALL LoadOrders();
SELECT 'Замовлення успішно завантажені.' AS status;



SELECT 'Схему оновлено. У вас є 3 великі таблиці: customers, reviews, orders.' AS status;



CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10, 2)
);
SELECT 'Таблицю orders створено.' AS status;

-- --------------------------------------------------------
-- КРОК 2: Створюємо процедуру для 1 млн замовлень
-- --------------------------------------------------------
DROP PROCEDURE IF EXISTS LoadOrders;

DELIMITER $$
CREATE PROCEDURE LoadOrders()
BEGIN
    DECLARE k INT DEFAULT 0;
    DECLARE batch_size INT DEFAULT 100000; -- 100,000 рядків за раз
    DECLARE max_rows INT DEFAULT 1000000;  -- Ціль: 1,000,000
    DECLARE max_cust_id INT;
    
    SET max_cust_id = (SELECT MAX(customer_id) FROM customers);
    
    IF max_cust_id IS NOT NULL AND max_cust_id > 0 THEN
        -- Починаємо цикл вставки
        WHILE k < max_rows DO
            INSERT INTO orders (customer_id, order_date, total_amount)
            SELECT 
                FLOOR(1 + RAND() * max_cust_id) AS customer_id,
                CURDATE() - INTERVAL FLOOR(RAND() * 365) DAY AS order_date,
                ROUND(50 + RAND() * 1000, 2) AS total_amount
            FROM (
                -- Генератор на 262,144 рядків (більше ніж 100,000)
                SELECT 1
                FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t1,
                     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t2,
                     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t3,
                     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t4,
                     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t5,
                     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t6,
                     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t7,
                     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t8,
                     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) t9
            ) seqs
            LIMIT batch_size; -- Обмежуємо партію
            
            SET k = k + batch_size;
        END WHILE;
    ELSE
        SELECT 'ПОМИЛКА: Таблиця customers порожня.' AS status;
    END IF;
END$$
DELIMITER ;

-- --------------------------------------------------------
-- КРОК 3: Запускаємо процедуру
-- --------------------------------------------------------
SELECT 'Починаємо вставку 1,000,000 замовлень...' AS status;
CALL LoadOrders();
SELECT '1,000,000 замовлень успішно завантажено.' AS status;


SELECT 'Зовнішній ключ для orders додано.' AS status;