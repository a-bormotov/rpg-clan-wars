WITH input AS (
  SELECT *
  FROM json_populate_recordset(
    NULL::record,
    %s
  ) AS t("userId" bigint, subtract int, clan text)
),

-- сумма averageSleepScore за нужный период по каждому игроку
sleep_penalty AS (
  SELECT
    ss."userId",
    SUM(COALESCE(ss."averageSleepScore", 0)) AS sleep_score_sum
  FROM sleep_summary ss
  WHERE ss."sleepDate"::date BETWEEN DATE '2025-08-15' AND DATE '2025-08-18'
  GROUP BY ss."userId"
),

-- считаем вклад игрока: vSleep из users.rpg минус subtract из JSON и минус сумма averageSleepScore
player_stats AS (
  SELECT
    i."userId",
    i.clan,
    COALESCE(u.username, i."userId"::text) AS username,
    GREATEST(
      COALESCE( (u.rpg::jsonb->'player'->>'vSleep')::int, 0)
      - COALESCE(i.subtract, 0)
      - COALESCE(sp.sleep_score_sum, 0),
      0
    ) AS player_result
  FROM input i
  LEFT JOIN users u        ON u.id = i."userId"
  LEFT JOIN sleep_penalty sp ON sp."userId" = i."userId"
),

clan_totals AS (   -- сумма по клану и место
  SELECT
    clan,
    SUM(player_result) AS clan_result_total,
    DENSE_RANK() OVER (ORDER BY SUM(player_result) DESC) AS clan_rank
  FROM player_stats
  GROUP BY clan
),

player_ranked AS ( -- место игрока внутри клана
  SELECT
    p.clan,
    p."userId",
    p.username,
    p.player_result,
    ROW_NUMBER() OVER (
      PARTITION BY p.clan
      ORDER BY p.player_result DESC, p."userId"
    ) AS player_rank
  FROM player_stats p
)

-- финальная таблица: сначала кланы, затем игроки
-- ВАЖНО: колонка "userId" остаётся текстовой и содержит username (как и раньше),
-- чтобы не менять структуру leaderboard.csv для сайта.
SELECT
  'clan'::text AS kind,
  ct.clan,
  ct.clan_rank,
  ct.clan_result_total,
  NULL::text AS "userId",
  NULL::int  AS player_rank,
  NULL::numeric AS player_result,
  0 AS order_in_group
FROM clan_totals ct

UNION ALL

SELECT
  'player'::text AS kind,
  pr.clan,
  ct.clan_rank,
  ct.clan_result_total,
  pr.username       AS "userId",    -- username в колонке "userId"
  pr.player_rank,
  pr.player_result,
  1 AS order_in_group
FROM player_ranked pr
JOIN clan_totals ct USING (clan)

ORDER BY clan_rank, order_in_group, player_rank NULLS FIRST, "userId";
