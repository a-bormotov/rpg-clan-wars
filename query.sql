WITH input AS (
  SELECT *
  FROM json_populate_recordset(
    NULL::record,
    %s
  ) AS t("userId" bigint, subtract int, clan text)
),
ur_agg AS (
  SELECT "userId", SUM(amount) AS amount
  FROM users_resources
  WHERE "resourceType" = 'vSleep'
  GROUP BY "userId"
),
player_stats AS (  -- each player's final vSleep after subtract
  SELECT
    i."userId",
    i.clan,
    GREATEST(COALESCE(u.amount, 0) - i.subtract, 0) AS player_result
  FROM input i
  LEFT JOIN ur_agg u ON u."userId" = i."userId"
),
clan_totals AS (   -- clan total and clan rank
  SELECT
    clan,
    SUM(player_result) AS clan_result_total,
    DENSE_RANK() OVER (ORDER BY SUM(player_result) DESC) AS clan_rank
  FROM player_stats
  GROUP BY clan
),
player_ranked AS ( -- player rank within clan
  SELECT
    p.clan,
    p."userId",
    p.player_result,
    ROW_NUMBER() OVER (PARTITION BY p.clan ORDER BY p.player_result DESC, p."userId") AS player_rank
  FROM player_stats p
)
-- final table: clan rows + player rows
SELECT
  'clan'::text AS kind,
  ct.clan,
  ct.clan_rank,
  ct.clan_result_total,
  NULL::bigint AS "userId",
  NULL::int    AS player_rank,
  NULL::numeric AS player_result,
  0 AS order_in_group
FROM clan_totals ct

UNION ALL

SELECT
  'player'::text AS kind,
  pr.clan,
  ct.clan_rank,
  ct.clan_result_total,
  pr."userId",
  pr.player_rank,
  pr.player_result,
  1 AS order_in_group
FROM player_ranked pr
JOIN clan_totals ct USING (clan)

ORDER BY clan_rank, order_in_group, player_rank NULLS FIRST, "userId";
