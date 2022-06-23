-- task 1

SELECT category.name, COUNT(film_category.category_id)
FROM film_category
JOIN category
ON film_category.category_id = category.category_id
GROUP BY category.name
ORDER BY COUNT(film_category.category_id) DESC;

-- task 2

SELECT actor.first_name, actor.last_name, COUNT(film_actor.actor_id)
FROM film_actor
JOIN actor
ON film_actor.actor_id = actor.actor_id
GROUP BY actor.first_name, actor.last_name
ORDER BY COUNT(film_actor.actor_id) DESC
LIMIT 10;

-- task 3

SELECT category.name, SUM(film.replacement_cost)
FROM film_category
JOIN film
ON film.film_id = film_category.film_id
JOIN category
ON category.category_id = film_category.category_id
GROUP BY category.name
ORDER BY SUM(replacement_cost) DESC
LIMIT 1;


--task 4

SELECT title
FROM film
LEFT JOIN inventory ON inventory.film_id = film.film_id
WHERE inventory.film_id IS NULL;

--task 5

SELECT actor.first_name, actor.last_name
FROM film_category
JOIN film_actor
ON film_actor.film_id = film_category.film_id
JOIN actor
ON actor.actor_id = film_actor.actor_id
WHERE film_category.category_id = 3
GROUP BY actor.first_name, actor.last_name
ORDER BY COUNT(film_actor.actor_id) DESC
FETCH FIRST 3 ROWS WITH TIES;


--task 6

SELECT city.city,
SUM(CASE WHEN customer.active = 1 THEN 1 ELSE 0 END) AS active_count,
SUM(CASE WHEN customer.active = 0 THEN 1 ELSE 0 END) AS no_active_count
FROM customer
JOIN address
ON address.address_id = customer.address_id
JOIN city
ON city.city_id = address.city_id
GROUP BY city.city
ORDER BY SUM(CASE WHEN customer.active = 0 THEN 1 ELSE 0 END) DESC;

--task 7



(SELECT category.name,
       SUM(CASE WHEN city.city LIKE 'a%' THEN film.rental_duration  ELSE 0 END) as sum_a,
       SUM(CASE WHEN city.city LIKE '%-%' THEN film.rental_duration  ELSE 0 END) as sum_
FROM rental
JOIN customer ON rental.customer_id = customer.customer_id
JOIN address ON address.address_id = customer.address_id
JOIN city ON city.city_id = address.city_id
JOIN inventory ON inventory.inventory_id = rental.inventory_id
JOIN film_category ON inventory.film_id = film_category.film_id
JOIN film ON film.film_id = inventory.film_id
JOIN category ON film_category.category_id = category.category_id
WHERE (city.city LIKE 'a%') OR (city.city LIKE '%-%')
GROUP BY category.name
ORDER BY sum_a DESC
LIMIT 1)
UNION ALL
(SELECT category.name,
       SUM(CASE WHEN city.city LIKE 'a%' THEN film.rental_duration  ELSE 0 END) as sum_a,
       SUM(CASE WHEN city.city LIKE '%-%' THEN film.rental_duration  ELSE 0 END) as sum_
FROM rental
JOIN customer ON rental.customer_id = customer.customer_id
JOIN address ON address.address_id = customer.address_id
JOIN city ON city.city_id = address.city_id
JOIN inventory ON inventory.inventory_id = rental.inventory_id
JOIN film_category ON inventory.film_id = film_category.film_id
JOIN film ON film.film_id = inventory.film_id
JOIN category ON film_category.category_id = category.category_id
WHERE (city.city LIKE 'a%') OR (city.city LIKE '%-%')
GROUP BY category.name
ORDER BY sum_ DESC
LIMIT 1)
;


