-- Phase 1: Seed SQL examples for few-shot learning
-- Generated from database/seed_queries.json
-- Summary = question (for question-based embedding similarity)

INSERT INTO sql_examples (question, sql_query, summary) VALUES
-- Easy queries
('What is the total revenue?',
 'SELECT SUM(amount) as total_revenue FROM payment;',
 'What is the total revenue?'),

('How many active customers do we have?',
 'SELECT COUNT(*) as active_customers FROM customer WHERE activebool = true;',
 'How many active customers do we have?'),

('How many rentals have been made in total?',
 'SELECT COUNT(*) as total_rentals FROM rental;',
 'How many rentals have been made in total?'),

('How many films do we have in each rating category?',
 'SELECT rating, COUNT(*) as film_count FROM film GROUP BY rating ORDER BY film_count DESC;',
 'How many films do we have in each rating category?'),

('What is the average rental duration in days?',
 'SELECT AVG(EXTRACT(EPOCH FROM (return_date - rental_date))/86400)::numeric(5,2) as avg_rental_days FROM rental WHERE return_date IS NOT NULL;',
 'What is the average rental duration in days?'),

('List all film categories',
 'SELECT name FROM category ORDER BY name;',
 'List all film categories'),

('How many films are in our inventory?',
 'SELECT COUNT(*) as inventory_count FROM inventory;',
 'How many films are in our inventory?'),

-- Medium queries
('Who are the top 5 customers by total spending?',
 'SELECT c.customer_id, c.first_name, c.last_name, SUM(p.amount) as total_spent FROM customer c JOIN payment p ON c.customer_id = p.customer_id GROUP BY c.customer_id, c.first_name, c.last_name ORDER BY total_spent DESC LIMIT 5;',
 'Who are the top 5 customers by total spending?'),

('What is the monthly revenue breakdown?',
 'SELECT DATE_TRUNC(''month'', payment_date) as month, SUM(amount) as revenue FROM payment GROUP BY month ORDER BY month;',
 'What is the monthly revenue breakdown?'),

('What are the most rented films?',
 'SELECT f.title, COUNT(r.rental_id) as rental_count FROM film f JOIN inventory i ON f.film_id = i.film_id JOIN rental r ON i.inventory_id = r.inventory_id GROUP BY f.film_id, f.title ORDER BY rental_count DESC LIMIT 10;',
 'What are the most rented films?'),

('How many customers have made more than 30 rentals?',
 'SELECT COUNT(*) as high_activity_customers FROM (SELECT customer_id, COUNT(*) as rental_count FROM rental GROUP BY customer_id HAVING COUNT(*) > 30) as active_customers;',
 'How many customers have made more than 30 rentals?'),

('What is the average payment amount per customer?',
 'SELECT AVG(total_spent)::numeric(5,2) as avg_customer_spend FROM (SELECT customer_id, SUM(amount) as total_spent FROM payment GROUP BY customer_id) as customer_totals;',
 'What is the average payment amount per customer?'),

('Which actors appear in the most films?',
 'SELECT a.first_name, a.last_name, COUNT(fa.film_id) as film_count FROM actor a JOIN film_actor fa ON a.actor_id = fa.actor_id GROUP BY a.actor_id, a.first_name, a.last_name ORDER BY film_count DESC LIMIT 5;',
 'Which actors appear in the most films?'),

('What is the revenue breakdown by day of week?',
 'SELECT TO_CHAR(payment_date, ''Day'') as day_of_week, SUM(amount) as revenue FROM payment GROUP BY TO_CHAR(payment_date, ''Day''), EXTRACT(DOW FROM payment_date) ORDER BY EXTRACT(DOW FROM payment_date);',
 'What is the revenue breakdown by day of week?'),

-- Hard queries
('Which film categories generate the most revenue?',
 'SELECT cat.name as category, SUM(p.amount) as revenue FROM category cat JOIN film_category fc ON cat.category_id = fc.category_id JOIN film f ON fc.film_id = f.film_id JOIN inventory i ON f.film_id = i.film_id JOIN rental r ON i.inventory_id = r.inventory_id JOIN payment p ON r.rental_id = p.rental_id GROUP BY cat.name ORDER BY revenue DESC LIMIT 5;',
 'Which film categories generate the most revenue?'),

('What percentage of rentals are returned late?',
 'SELECT (COUNT(*) FILTER (WHERE return_date > rental_date + (f.rental_duration || '' days'')::interval) * 100.0 / COUNT(*))::numeric(5,2) as late_return_pct FROM rental r JOIN inventory i ON r.inventory_id = i.inventory_id JOIN film f ON i.film_id = f.film_id WHERE return_date IS NOT NULL;',
 'What percentage of rentals are returned late?'),

('Which customers have not made any rentals in the last 3 months of data?',
 'SELECT c.customer_id, c.first_name, c.last_name FROM customer c WHERE c.customer_id NOT IN (SELECT DISTINCT customer_id FROM rental WHERE rental_date >= (SELECT MAX(rental_date) - INTERVAL ''3 months'' FROM rental)) ORDER BY c.customer_id LIMIT 10;',
 'Which customers have not made any rentals in the last 3 months of data?'),

-- Expert queries
('What is the running total of revenue by month?',
 'SELECT month, monthly_revenue, SUM(monthly_revenue) OVER (ORDER BY month) as running_total FROM (SELECT DATE_TRUNC(''month'', payment_date) as month, SUM(amount) as monthly_revenue FROM payment GROUP BY DATE_TRUNC(''month'', payment_date)) as monthly ORDER BY month;',
 'What is the running total of revenue by month?'),

('Rank customers by their total spending',
 'SELECT customer_id, first_name, last_name, total_spent, RANK() OVER (ORDER BY total_spent DESC) as spending_rank FROM (SELECT c.customer_id, c.first_name, c.last_name, SUM(p.amount) as total_spent FROM customer c JOIN payment p ON c.customer_id = p.customer_id GROUP BY c.customer_id, c.first_name, c.last_name) as customer_spending ORDER BY spending_rank LIMIT 10;',
 'Rank customers by their total spending'),

('What is the month-over-month revenue growth rate?',
 'WITH monthly_revenue AS (SELECT DATE_TRUNC(''month'', payment_date) as month, SUM(amount) as revenue FROM payment GROUP BY DATE_TRUNC(''month'', payment_date)) SELECT month, revenue, LAG(revenue) OVER (ORDER BY month) as prev_revenue, ((revenue - LAG(revenue) OVER (ORDER BY month)) / LAG(revenue) OVER (ORDER BY month) * 100)::numeric(5,2) as growth_pct FROM monthly_revenue ORDER BY month;',
 'What is the month-over-month revenue growth rate?')

ON CONFLICT DO NOTHING;

-- Note: Embeddings will be generated by MCP server on startup (Phase 2)
