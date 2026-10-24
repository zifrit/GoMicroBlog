-- Проверяем планы запросов, не меняя данные приложения. На маленьких таблицах PostgreSQL может выбрать последовательное сканирование.
ANALYZE users;
ANALYZE posts;
ANALYZE likes;

-- Индекс user_id создаётся для каждой партиции; границы по дате позволяют исключать лишние партиции.
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, text FROM posts
WHERE user_id = 'alice'
  AND created_at >= '2026-01-01 00:00:00+00'
  AND created_at < '2027-01-01 00:00:00+00';

-- Начальные столбцы likes_pkey используются при поиске лайков поста.
EXPLAIN (ANALYZE, BUFFERS)
SELECT user_id FROM likes WHERE post_id = 1;

EXPLAIN (ANALYZE, BUFFERS)
SELECT post_id FROM likes WHERE user_id = 'alice';
