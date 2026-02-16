
DESCRIBE SELECT * FROM read_json_auto("C:\Users\denys\Downloads\hw1_data\Cell_Phones_and_Accessories_5.json", maximum_object_size=268435456 );

CREATE or Replace TABLE  reviews AS
SELECT
    reviewerID,
    asin,
    reviewerName,
    helpful[1] AS helpful_votes,
    helpful[2] AS total_votes,
    overall,
    summary,
    to_timestamp(unixReviewTime) AS review_date,
    reviewText
FROM read_json_auto("C:\Users\denys\Downloads\hw1_data\Cell_Phones_and_Accessories_5.json", maximum_object_size=268435456);


Select* from reviews;



with month as (
    select date_trunc('month', review_date) AS review_month,
        count(*) as mention
    from reviews
    where reviewText ILIKE '%trump%'
    group by review_month)
Select review_month, mention,
    SUM(mention) OVER (order by review_month
        rows between unbounded preceding and current row
    )  cumulative_count
from month
Order by review_month Desc;



with month_2 as (
    select date_trunc('year', review_date) AS votes_year,
        sum(total_votes) as votes
    from reviews
    group by votes_year),
previous as (
    select votes_year,votes,
           lag(votes) over(order by votes_year) as previous_votes
    from month_2
)
Select votes_year,
       votes,
       previous_votes,
       ((votes - previous_votes) * 100.0) / NULLIF(previous_votes, 0) as calcul
from previous
Order by votes_year Desc;





