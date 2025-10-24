-- ВАРІАНТ 1: ГАРАНТОВАНО ПОВІЛЬНИЙ (Корельовані підзапити)

SELECT 
    c.customer_name,
    c.region,
    
    -- !! ПІДЗАПИТ 1 (Виконується для КОЖНОГО рядка) !!
    (SELECT SUM(o.total_amount)
     FROM orders o
     WHERE o.customer_id = c.customer_id -- (кореляція)
    ) AS total_spent,
    
    -- !! ПІДЗАПИТ 2 (Виконується для КОЖНОГО рядка) !!
    (SELECT COUNT(r.review_id)
     FROM reviews r
     JOIN products p ON r.product_id = p.product_id
     WHERE r.customer_id = c.customer_id -- (кореляція)
       AND r.rating = 1
       AND p.category = 'Електроніка'
    ) AS angry_reviews_count
    
FROM 
    -- 1. Основний запит: отримуємо ~200,000 клієнтів
    customers c
WHERE
    c.region = 'Центр'
    
ORDER BY 
    total_spent DESC
LIMIT 10;


Create index idx_orders_customer_id on orders(customer_id);
create index idx_reviews_customer_id on reviews(customer_id);
create index idx_products_category on products(category);


With total_spent as (
    Select 
        o.customer_id,
        sum(o.total_amount) AS total_spent
    from orders o
    group by o.customer_id
),

angry as (
    SELECT 
        r.customer_id,
        count(*) as angry_reviews_count
    From reviews r
    JOIN products p on r.product_id = p.product_id
    where r.rating = 1 and p.category = 'Електроніка'
    group by r.customer_id
)
	
select
    c.customer_name,
    c.region,
    ts.total_spent,
   coalesce(a.angry_reviews_count, 0) as angry_reviews_count 
From customers c
Left Join total_spent ts on ts.customer_id = c.customer_id
left join  angry a on a.customer_id = c.customer_id
where c.region = 'Центр'
order by  total_spent desc
Limit 10;
