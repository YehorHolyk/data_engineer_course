/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT
    cat.name,
    COUNT(fc.film_id) AS film_count
FROM
    category cat
    LEFT JOIN film_category fc ON cat.category_id = fc.category_id
GROUP BY
    cat.category_id
ORDER BY
    film_count DESC;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
/*
query explanation:
In order to see all rented films with actors, `rental` and `actor` tables were connected
through intermediate tables. Result was grouped by `actor_id`, `first_name` and `last_name` and
ordered by COUNT(`rental_id`) DESC to see actors with count of rented films in which they took part in descending order.
`FETCH FIRST 10 ROWS WITH TIES;` allows to see top 10 actors with the biggest count,
and extra actors who have the same count as the actor in the 10th position (just in case).
*/
SELECT
    a.actor_id,
    a.first_name,
    a.last_name
FROM
    rental rent
    INNER JOIN inventory i      ON rent.inventory_id = i.inventory_id
    INNER JOIN film f           ON i.film_id = f.film_id
    INNER JOIN film_actor fa    ON f.film_id = fa.film_id
    INNER JOIN actor a          ON fa.actor_id = a.actor_id
GROUP BY
    a.actor_id,
    a.first_name,
    a.last_name
ORDER BY
    COUNT(rent.rental_id) DESC
FETCH FIRST 10 ROWS WITH TIES;


/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
/*
query explanation:
In order to see all rented films with categories, `rental` and `category` tables were connected
through intermediate tables. Result was grouped by `c.category_id` and `c.name` and
ordered by SUM(`f.rental_rate`) DESC to see categories with total amount of rent price in descending order.
`FETCH FIRST 1 ROWS WITH TIES;` allows to see top 1 category with the biggest total amount of rent price,
and extra categories which have the same total amount as the category in the 1st position (just in case).
*/
SELECT
    c.category_id,
    c.name
FROM
    rental rent
    INNER JOIN inventory i      ON i.inventory_id = rent.inventory_id
    INNER JOIN film f           ON f.film_id = i.film_id
    INNER JOIN film_category fc ON f.film_id = fc.film_id
    INNER JOIN category c       ON c.category_id = fc.category_id
GROUP BY
    c.category_id,
    c.name
ORDER BY
    SUM(f.rental_rate) DESC
FETCH FIRST 1 ROWS WITH TIES;


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT
    f.title
FROM
    film f
    LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE
    i.inventory_id IS NULL;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
/*
query explanation:
In order to see all actors with categories of films in which they took part, `actor` and `category` tables were
connected through intermediate tables. Result was filtered by c.name = 'Children',
grouped by `actor_id`, `first_name` and `last_name` and ordered by COUNT(`a.actor_id`) DESC
to see actors with count of films in which they took part.
`FETCH FIRST 3 ROWS WITH TIES;` allows to see top 3 actors with the biggest count,
and extra actors who have the same count as the actor in the 3d position (just in case).
*/
SELECT
    a.actor_id,
    a.first_name,
    a.last_name,
    COUNT(a.actor_id)
FROM
    actor a
    INNER JOIN film_actor fa    ON a.actor_id = fa.actor_id
    INNER JOIN film f           ON f.film_id = fa.film_id
    INNER JOIN film_category fc ON f.film_id = fc.film_id
    INNER JOIN category c       ON c.category_id = fc.category_id
WHERE
    c.name = 'Children'
GROUP BY
    a.actor_id,
    a.first_name,
    a.last_name
ORDER BY
    COUNT(a.actor_id) DESC
FETCH FIRST 3 ROWS WITH TIES;
