
DESCRIBE SELECT unnest(reviews) FROM read_json_auto("C:\Users\denys\Downloads\steam_2025_5k-dataset-reviews_20250901.json\steam_2025_5k-dataset-reviews_20250901.json", maximum_object_size=268435456 );

CREATE OR REPLACE TABLE reviews AS
SELECT
    appid,
    r.recommendationid AS review_id,
    r.review AS review_text,
    r.language,
    r.voted_up AS is_positive,
    r.votes_up AS helpful_votes,
    to_timestamp(r.timestamp_created) AS created_at,
    r.author.steamid AS author_id,
    r.author.playtime_forever / 60.0 AS playtime_hours_total,
    r.author.num_games_owned AS author_games_count

FROM (
    SELECT
        g.appid,
        unnest(g.review_data.reviews) AS r
    FROM (
        SELECT unnest(reviews) AS g
        FROM read_json_auto("C:\Users\denys\Downloads\steam_2025_5k-dataset-reviews_20250901.json\steam_2025_5k-dataset-reviews_20250901.json", maximum_object_size=268435456)
    )
    WHERE g.review_data.success = 1
);

SELECT *from reviews




CREATE OR REPLACE TABLE games AS
SELECT
    g.appid,
    g.name_from_applist AS original_name,
    g.app_details.data.name AS name,
    g.app_details.data.type AS type,
    g.app_details.data.is_free AS is_free,
    g.app_details.data.release_date.date AS release_date,
    g.app_details.data.price_overview.final AS price,
    g.app_details.data.price_overview.currency AS currency,
    g.app_details.data.price_overview.discount_percent AS discount,
    g.app_details.data.short_description AS description,
    g.app_details.data.website AS website,
    g.app_details.data.header_image AS image_url,
    unnest(g.app_details.data.developers) AS developers,
    unnest(list_transform(g.app_details.data.genres, x -> x.description))AS genres

FROM (
    SELECT unnest(games) AS g
    FROM read_json_auto("C:/Users/denys/Downloads/steam_2025_5k-dataset-games_20250831.json/steam_2025_5k-dataset-games_20250831.json", maximum_object_size=268435456) -- <-- ВСТАВ ТУТ НАЗВУ ФАЙЛУ
)
WHERE g.app_details.success = true;

Select* from games;



SELECT
    g.name,
    COUNT(*) AS review_count
FROM reviews r
JOIN games g ON r.appid = g.appid
GROUP BY g.name
ORDER BY review_count DESC
LIMIT 20; -- review count by name, most reviewed games


SELECT
    genres,
    AVG(price)AS avg_price,
FROM games
WHERE price IS NOT NULL
GROUP BY genres
ORDER BY avg_price DESC; -- The most expensive games but without currency standartization




SELECT
    CASE WHEN is_free = true THEN 'free_game'
    ELSE 'paid_game' END AS status,
    COUNT(DISTINCT appid) AS games_count
FROM games
GROUP BY is_free; -- free/paid games share



SELECT DISTINCT
    name,
    discount,
    price,
    currency
FROM games
WHERE discount > 50
ORDER BY discount DESC; -- games with the biggest discount


SELECT
    developers ,
    COUNT(*) AS games_count
FROM games
where developers IS NOT NULL
GROUP BY developers
ORDER BY games_count DESC
LIMIT 20; -- most productive developers


COPY games TO 'games_data.csv' (HEADER, DELIMITER ',');
